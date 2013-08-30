package com.twitter.ambrose.model;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
public class Test {
	
	public static void main(String[] args) {
	    // QL execution stuff
		System.out.println(ConfVars.SCRATCHDIR.varname + "" + ConfVars.SCRATCHDIR.defaultVal);
		//HiveConf.getVar(conf, ConfVars.SCRATCHDIR);
	}
}
