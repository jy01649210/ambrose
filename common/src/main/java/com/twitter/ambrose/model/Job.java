/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.ambrose.model;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Objects;
import com.twitter.ambrose.model.hadoop.CounterGroup;
import com.twitter.ambrose.model.hadoop.MapReduceJobState;
import com.twitter.ambrose.util.JSONUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Class that encapsulates all information related to a run of a job. A job might have job
 * configuration and job metric data. Job metrics represents job data that is produced after the
 * conclusion of a job.
 *
 * @author billg
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "runtime")
@JsonSubTypes({
    @JsonSubTypes.Type(value=com.twitter.ambrose.model.Job.class, name="hive")
})
public class Job {
  private String id;
  private String[] aliases;
  private String[] features;
  private MapReduceJobState mapReduceJobState;
  private Map<String, CounterGroup> counterGroupMap;
  private Properties configuration;
  private Map<String, Number> metrics;

  protected Job() { }

  @JsonCreator
  public Job(@JsonProperty("id") String id,
		  @JsonProperty("aliases") String[] aliases,
	      @JsonProperty("features") String[] features,
	      @JsonProperty("mapReduceJobState") MapReduceJobState mapReduceJobState,
	      @JsonProperty("counterGroupMap") Map<String, CounterGroup> counterGroupMap,
      @JsonProperty("configuration") Properties configuration,
      @JsonProperty("metrics") Map<String, Number> metrics) {
	  this.id = id;
	    this.aliases = aliases;
	    this.features = features;
	    this.mapReduceJobState = mapReduceJobState;
	    this.counterGroupMap = counterGroupMap;
	    this.metrics = metrics;
	    this.configuration = configuration;
  }

  public String getId() { return id; }
  public void setId(String id) { this.id = id; }

  public Properties getConfiguration() { return configuration; }
  public void setConfiguration(Properties configuration) { this.configuration = configuration; }

  public Map<String, Number> getMetrics() { return metrics; }
  protected void setMetrics(Map<String, Number> metrics) { this.metrics = metrics; }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, configuration, metrics);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Job that = (Job) obj;
    return Objects.equal(id, that.id)
        && Objects.equal(configuration, that.configuration)
        && Objects.equal(metrics, that.metrics);
  }

  public String toJson() throws IOException {
    return JSONUtil.toJson(this);
  }

  public static Job fromJson(String json) throws IOException {
    return JSONUtil.toObject(json, new TypeReference<Job>() { });
  }

public String[] getAliases() {
	return aliases;
}

public void setAliases(String[] aliases) {
	this.aliases = aliases;
}

public String[] getFeatures() {
	return features;
}

public void setFeatures(String[] features) {
	this.features = features;
}

public MapReduceJobState getMapReduceJobState() {
	return mapReduceJobState;
}

public void setMapReduceJobState(MapReduceJobState mapReduceJobState) {
	this.mapReduceJobState = mapReduceJobState;
}

public Map<String, CounterGroup> getCounterGroupMap() {
	return counterGroupMap;
}

public void setCounterGroupMap(Map<String, CounterGroup> counterGroupMap) {
	this.counterGroupMap = counterGroupMap;
}

}
