package com.an.mrproject.pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRPairDriver {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();

		Job job = new Job(configuration, "MRProject Pair Approach Finding Relative Frequency");

		job.setJarByClass(MRPairDriver.class);
		job.setNumReduceTasks(2);
		job.setMapperClass(PairMapper.class);
		job.setPartitionerClass(DataPartitioner.class);
		job.setReducerClass(PairReducer.class);

		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(PairWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}