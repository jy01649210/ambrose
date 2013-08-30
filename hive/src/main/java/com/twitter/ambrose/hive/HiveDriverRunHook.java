package com.twitter.ambrose.hive;

/**
 * HiveDriverRunHook allows Hive to be extended with custom
 * logic for processing commands.
 *
 *<p>
 *
 * Note that the lifetime of an instantiated hook object is scoped to
 * the analysis of a single statement; hook instances are never reused.
 */
public interface HiveDriverRunHook extends Hook {
  /**
   * Invoked before Hive begins any processing of a command in the Driver,
   * notably before compilation and any customizable performance logging.
   */
  public void preDriverRun(
    HiveDriverRunHookContext hookContext) throws Exception;

  /**
   * Invoked after Hive performs any processing of a command, just before a
   * response is returned to the entity calling the Driver.
   */
  public void postDriverRun(
    HiveDriverRunHookContext hookContext) throws Exception;
}
