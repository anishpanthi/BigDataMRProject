package com.an.mrproject.pair;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRPairDriver {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Pair Approach To Find Realtive Frequency");

		job.setJarByClass(MRPairDriver.class);
		job.setNumReduceTasks(2);

		job.setMapperClass(PairMapper.class);
		job.setPartitionerClass(DataPartitioner.class);
		job.setReducerClass(PairReducer.class);

		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(PairWritable.class);
//		job.setOutputValueClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}