/*
Copyright 2013, Lorand Bendig

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.twitter.ambrose.hive;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.PostExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import com.twitter.ambrose.model.Workflow;
import com.twitter.ambrose.model.Event.WorkflowProgressField;
import com.twitter.ambrose.hive.reporter.EmbeddedAmbroseHiveProgressReporter;

import static com.twitter.ambrose.hive.reporter.AmbroseHiveReporterFactory.getEmbeddedProgressReporter;

/**
 * Hook invoked when a workflow succeeds. If the last statement (workflow) of
 * the script was executed, it waits for
 * <code>{@value #POST_SCRIPT_SLEEP_SECS_PARAM}</code> seconds before exiting
 * otherwise returns and the processing moves on to the next workflow. <br>
 * Called by the main thread
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 * 
 */
public class AmbroseHiveFinishHook implements PostExecute {

  private static final Log LOG = LogFactory.getLog(AmbroseHiveFinishHook.class);
  private static final String POST_SCRIPT_SLEEP_SECS_PARAM = "ambrose.post.script.sleep.seconds";

  /** Last workflow in the script to be processed */
  private final String lastCmd;

  /** Running job information */
  private final JobClient jobClient;

  
  private final Map<WorkflowProgressField, String> eventData = 
		    new HashMap<WorkflowProgressField, String>(1);

  public AmbroseHiveFinishHook() throws IOException {
    lastCmd = getLastCmd();
    Configuration conf = SessionState.get().getConf();
    this.jobClient = new JobClient(new JobConf(conf));
  }

  private void outputStatsData(Workflow workflowInfo) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Collected stats for script:\n" + Workflow.toJSON(workflowInfo));
      }
    }
    catch (IOException e) {
      LOG.error("Error while outputting workflowInfo", e);
    }
  }

  private void displayStatistics() {
    EmbeddedAmbroseHiveProgressReporter reporter = getEmbeddedProgressReporter();
    Map<String, String> jobIdToNodeId = reporter.getJobIdToNodeId();
    LOG.info("MapReduce Jobs Launched: ");
    List<MapRedStats> lastMapRedStats = AmbroseHiveStatPublisher.getLastMapRedStats();
    for (int i = 0; i < lastMapRedStats.size(); i++) {
      MapRedStats mrStats = lastMapRedStats.get(i);
      String jobId = mrStats.getJobId();
      String nodeId = jobIdToNodeId.get(jobId);
      StringBuilder sb = new StringBuilder();
      sb.append("Job ")
        .append(i)
        .append(" (")
        .append(jobId)
        .append(", ")
        .append(nodeId)
        .append("): ")
        .append(mrStats);
      LOG.info(sb.toString());
    }
  }

  private boolean isLastCommandProcessed(String currentCmd) {
    currentCmd = StringUtils.trim(currentCmd.replaceAll("\\n|\\r", ""));
    if (currentCmd.equals(lastCmd)) {
      return true;
    }
    return false;
  }

  private String getLastCmd() {
    CliSessionState cliss = (CliSessionState) SessionState.get();
    Scanner scanner = null;
    try {
      scanner = new Scanner(new File(cliss.fileName));
    }
    catch (FileNotFoundException e) {
      LOG.error("Can't find Hive script", e);
    }
    if (scanner == null) {
      return null;
    }
    Pattern delim = Pattern.compile(";");
    scanner.useDelimiter(delim);
    String lastLine = null;
    while (scanner.hasNext()) {
      String line = StringUtils.trim(scanner.next().replaceAll("\\n|\\r", ""));
      if (line.length() != 0 && !line.startsWith("--")) {
        lastLine = line;
      }
    }
    return lastLine;
  }
  

@Override
public void run(SessionState sess, Set<ReadEntity> inputs,
		Set<WriteEntity> outputs, LineageInfo lInfo, UserGroupInformation ugi)
		throws Exception {
	Configuration conf = sess.getConf();

    EmbeddedAmbroseHiveProgressReporter reporter = getEmbeddedProgressReporter();
    String workflowVersion = reporter.getWorkflowVersion();

    String queryId = AmbroseHiveUtil.getHiveQueryId(conf);
    if (workflowVersion == null) {
      LOG.warn("ScriptFingerprint not set for this script - not saving stats.");
    }
    else {
      Workflow workflow = new Workflow(queryId, workflowVersion, reporter.getJobs());
      outputStatsData(workflow);
      reporter.flushJsonToDisk();
    }
        
    displayStatistics();

    if (!isLastCommandProcessed(sess.getCmd())) {
      return;
    }

    reporter.restoreEventStack(queryId);
	
}

}
