package com.twitter.ambrose.hive;

import java.util.Map;

public interface ClientStatsPublisher {

  public void run(Map<String, Double> counterValues, String jobID);

}
