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

import static com.twitter.ambrose.hive.reporter.AmbroseHiveReporterFactory.getEmbeddedProgressReporter;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import com.twitter.ambrose.hive.reporter.EmbeddedAmbroseHiveProgressReporter;
import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Event.WorkflowProgressField;
import com.twitter.ambrose.model.Job;

/**
 * Hook invoked before running a workflow. <br>
 * Constructs DAGNode representation and initializes
 * {@link com.twitter.ambrose.hive.HiveProgressReporter HiveProgressReporter} <br>
 * Called by the main thread
 * 
 * @author Lorand Bendig <lbendig@gmail.com>
 * 
 */
public class AmbroseHivePreHook implements PreExecute {

    private static final Log LOG = LogFactory.getLog(AmbroseHivePreHook.class);

    /** Timeout in seconds for waiting between two workflows */
    private static final String WF_BETWEEN_SLEEP_SECS_PARAM = "ambrose.wf.between.sleep.seconds";
    private static final String SCRIPT_STARTED_PARAM = "ambrose.script.started";
    
    @Override
	public void run(SessionState session, Set<ReadEntity> inputs,
			Set<WriteEntity> outputs, UserGroupInformation ugi)
			throws Exception {
    	String queryId = AmbroseHiveUtil.getHiveQueryId(session.getConf());
        EmbeddedAmbroseHiveProgressReporter reporter = getEmbeddedProgressReporter();
        HiveDAGTransformer transformer = new HiveDAGTransformer(session, getQueryPlan(session.getConf(),session.getCmd()));
        //conditional tasks may be filtered out by Hive at runtime. We them as
        //'complete'
        Map<String, DAGNode<Job>> nodeIdToDAGNode = reporter.getNodeIdToDAGNode();
        sendFilteredJobsStatus(queryId, reporter, nodeIdToDAGNode);
        if (transformer.getTotalMRJobs() == 0) {
            return;
        }
    	

        waitBetween(session, reporter, queryId);
        
        nodeIdToDAGNode = transformer.getNodeIdToDAGNode();
        reporter.setNodeIdToDAGNode(nodeIdToDAGNode);
        reporter.setTotalMRJobs(transformer.getTotalMRJobs());
        reporter.sendDagNodeNameMap(queryId, nodeIdToDAGNode);
		
	}

    /**
     * Waiting <tt>ambrose.wf.between.sleep.seconds</tt> before processing the
     * next statement (workflow) in the submitted script
     * 
     * @param hookContext
     * @param reporter
     * @param queryId
     */
    private void waitBetween(SessionState session, EmbeddedAmbroseHiveProgressReporter reporter, String queryId) {

        Configuration conf = session.getConf();
        boolean justStarted = conf.getBoolean(SCRIPT_STARTED_PARAM, true);
        if (justStarted) {
            conf.setBoolean(SCRIPT_STARTED_PARAM, false);
        }
        else {
            // sleeping between workflows
            int sleepTimeMs = conf.getInt(WF_BETWEEN_SLEEP_SECS_PARAM, 10);
            try {

                LOG.info("One workflow complete, sleeping for " + sleepTimeMs
                        + " sec(s) before moving to the next one if exists. Hit ctrl-c to exit.");
                Thread.sleep(sleepTimeMs * 1000L);
                
                //send progressbar reset event
                Map<WorkflowProgressField, String> eventData = 
                  new HashMap<WorkflowProgressField, String>(1);
                eventData.put(WorkflowProgressField.workflowProgress, "0");
                reporter.pushEvent(queryId, new Event.WorkflowProgressEvent(eventData));
                
                reporter.saveEventStack(queryId);
                reporter.reset();
            }
            catch (InterruptedException e) {
                LOG.warn("Sleep interrupted", e);
            }
        }
    }

    private void sendFilteredJobsStatus(String queryId, 
        EmbeddedAmbroseHiveProgressReporter reporter, Map<String, DAGNode<Job>> nodeIdToDAGNode) {
        
        if (nodeIdToDAGNode == null) {
            return;
        }
        
        Map<WorkflowProgressField, String> eventData = 
            new HashMap<Event.WorkflowProgressField, String>(1);

        int skipped = 0;
        for (DAGNode<Job> dagNode : nodeIdToDAGNode.values()) {
            Job job = dagNode.getJob();
            // filtered jobs don't have assigned jobId
            if (job.getId() != null) {
                continue;
            }
            String nodeId = dagNode.getName();
            job.setId(AmbroseHiveUtil.asDisplayId(queryId, "filtered out", nodeId));
            reporter.addJobIdToProgress(nodeId, 100);
            reporter.pushEvent(queryId, new Event.JobFinishedEvent(dagNode));
            skipped++;
        }
        // sleep so that all these events will be visible on GUI before going on
        try {
            Thread.sleep(skipped * 1000L);
        }
        catch (InterruptedException e) {
            LOG.warn("Sleep interrupted", e);
        }

        eventData.put(WorkflowProgressField.workflowProgress,
                Integer.toString(reporter.getOverallProgress()));
        reporter.pushEvent(queryId, new Event.WorkflowProgressEvent(eventData));

    }
    
    /**
     * Get the queryPlan based on the hive conf and query command.
     * PreExecute hook interface didn't provide QueryPlan, below code comes from Driver.java
     *
     * @param HiveConf conf, String command
     */
    public QueryPlan getQueryPlan(HiveConf conf, String command) {
      Utilities.PerfLogBegin(LOG, "compile");

      TaskFactory.resetId();

      try {
    	Context ctx = new Context(conf);
        ctx.setTryCount(Integer.MAX_VALUE);

        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(command, ctx);
        tree = ParseUtils.findRootNonNullToken(tree);

        SemanticAnalyzerFactory sfactory = new SemanticAnalyzerFactory(conf);
        BaseSemanticAnalyzer sem = sfactory.get(tree);
        List<AbstractSemanticAnalyzerHook> saHooks = getSemanticAnalyzerHooks(conf);
        // Do semantic analysis and plan generation
        if (saHooks != null) {
          HiveSemanticAnalyzerHookContext hookCtx = new HiveSemanticAnalyzerHookContextImpl();
          hookCtx.setConf(conf);
          hookCtx.setContext(ctx);
          for (AbstractSemanticAnalyzerHook hook : saHooks) {
            tree = hook.preAnalyze(hookCtx, tree);
          }
          sem.analyze(tree, ctx);
          for (AbstractSemanticAnalyzerHook hook : saHooks) {
            hook.postAnalyze(hookCtx, sem.getRootTasks(), sem.getFetchTask());
          }
        } else {
          sem.analyze(tree, ctx);
        }


        LOG.info("Semantic Analysis Completed");

        // validate the plan
        sem.validate();

        QueryPlan plan = new QueryPlan(command, sem);
        // initialize FetchTask right here
        if (plan.getFetchTask() != null) {
          plan.getFetchTask().initialize(conf, plan, null);
        }

        // get the output schema
        //Schema schema = getSchema(sem, conf);

        // test Only - serialize the query plan and deserialize it
        if ("true".equalsIgnoreCase(System.getProperty("test.serialize.qplan"))) {

          String queryPlanFileName = ctx.getLocalScratchDir(true) + Path.SEPARATOR_CHAR
              + "queryplan.xml";
          LOG.info("query plan = " + queryPlanFileName);
          queryPlanFileName = new Path(queryPlanFileName).toUri().getPath();

          // serialize the queryPlan
          FileOutputStream fos = new FileOutputStream(queryPlanFileName);
          Utilities.serializeQueryPlan(plan, fos);
          fos.close();

          // deserialize the queryPlan
          FileInputStream fis = new FileInputStream(queryPlanFileName);
          QueryPlan newPlan = Utilities.deserializeQueryPlan(fis, conf);
          fis.close();

          // Use the deserialized plan
          plan = newPlan;
        }

        // initialize FetchTask right here
        if (plan.getFetchTask() != null) {
          plan.getFetchTask().initialize(conf, plan, null);
        }

        //if (authorizeEnabled) {
          SessionState ss = SessionState.get();
          assert ss != null;
          sem.authorize(ss.getAuthorizer(), ss.getUserName());
        //}
        return plan;
      } catch (Exception e) {
        String errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
        LOG.info(errorMessage + "\n"
            + org.apache.hadoop.util.StringUtils.stringifyException(e));
        return null;
      } finally {
        Utilities.PerfLogEnd(LOG, "compile");
      }
    }
    
    private List<AbstractSemanticAnalyzerHook> getSemanticAnalyzerHooks(HiveConf conf) throws Exception {
        ArrayList<AbstractSemanticAnalyzerHook> saHooks = new ArrayList<AbstractSemanticAnalyzerHook>();
        String pestr = conf.getVar(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK);
        if (pestr == null) {
          return saHooks;
        }
        pestr = pestr.trim();
        if (pestr.equals("")) {
          return saHooks;
        }

        String[] peClasses = pestr.split(",");

        for (String peClass : peClasses) {
          try {
            AbstractSemanticAnalyzerHook hook = HiveUtils.getSemanticAnalyzerHook(conf, peClass);
            saHooks.add(hook);
          } catch (HiveException e) {
            LOG.info("Pre Exec Hook Class not found:" + e.getMessage());
            throw e;
          }
        }

        return saHooks;
      }
    
}
