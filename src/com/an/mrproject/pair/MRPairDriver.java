package com.an.mrproject.pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRPairDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		Configuration configuration = new Configuration();
		int exitCode = ToolRunner.run(configuration, new MRPairDriver(), args);
		System.exit(exitCode);

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
