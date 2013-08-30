package com.twitter.ambrose.hive;

import static com.twitter.ambrose.hive.reporter.AmbroseHiveReporterFactory.getEmbeddedProgressReporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.PreJobHook;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;

import com.twitter.ambrose.hive.reporter.EmbeddedAmbroseHiveProgressReporter;
import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.Event.WorkflowProgressField;

public class AmbroseHivePreJob implements PreJobHook {

	  private static final Log LOG = LogFactory.getLog(AmbroseHiveStatPublisher.class);

	  private final Map<WorkflowProgressField, String> eventData = 
	    new HashMap<WorkflowProgressField, String>(1);

	  public AmbroseHivePreJob() throws IOException {
	    Configuration conf = SessionState.get().getConf();
	  }
	  
	  @Override
		public void run(SessionState session, QueryPlan queryPlan, JobConf job,
				Integer taskId) throws Exception {

		  	Map<String, Double> counterValue = new HashMap<String, Double>();
		  	    // send job statistics to the Ambrose server
		  	send(job, counterValue);
			
		}

	  private void send(JobConf jobConf, Map<String, Double> counterValues) {
	       
		    EmbeddedAmbroseHiveProgressReporter reporter = getEmbeddedProgressReporter();

		    Configuration conf = SessionState.get().getConf();
		    String queryId = AmbroseHiveUtil.getHiveQueryId(conf);		    
		    Map<String, DAGNode<Job>> nodeIdToDAGNode = reporter.getNodeIdToDAGNode();
		    String jobName = jobConf.getJobName();
		    String prefix = "";
//		    AmbroseHiveUtil.getNodeIdFromNodeName(jobConf, jobConf.getJobName()) 
//		    jobConf.getJobName() didn't equal to runningJob.getJobName, it contains prefix of the nodeName
		    for(String nodeKey : nodeIdToDAGNode.keySet()) {
		    	prefix = nodeKey.split("_")[0];
		    	if(jobName.contains(prefix)) break;
		    }
		    String nodeId = prefix + "_" + AmbroseHiveUtil.getHiveQueryId(jobConf);
		    
		    DAGNode<Job> dagNode = nodeIdToDAGNode.get(nodeId);
		    if (dagNode == null) {
		      LOG.warn("jobStartedNotification - unrecorgnized operator name found for " + "jobId "
		          + jobConf);
		      return;
		    }
		    HiveJob job = (HiveJob) dagNode.getJob();
		    // a job has been started
		    if (job.getId() == null) {
		      // job identifier on GUI
		      reporter.pushEvent(queryId, new Event.JobStartedEvent(dagNode));
		      Event<DAGNode<? extends Job>> event = new Event.JobProgressEvent(dagNode);
		      pushWorkflowProgress(queryId, reporter);
		      reporter.pushEvent(queryId, event);
		    }

		  }

	  private void pushWorkflowProgress(String queryId, EmbeddedAmbroseHiveProgressReporter reporter) {
	    eventData.put(WorkflowProgressField.workflowProgress,
	        Integer.toString(reporter.getOverallProgress()));
	    reporter.pushEvent(queryId, new Event.WorkflowProgressEvent(eventData));
	  }

	}