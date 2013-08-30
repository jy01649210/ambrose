package com.twitter.ambrose.hive;

import org.apache.hadoop.conf.Configurable;

/**
 * Context information provided by Hive to implementations of
 * HiveDriverRunHook.
 */
public interface HiveDriverRunHookContext extends Configurable{
  public String getCommand();
  public void setCommand(String command);
}