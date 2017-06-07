package com.an.mrproject.pair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class DataPartitioner extends Partitioner<PairWritable, IntWritable> {

	@Override
	public int getPartition(PairWritable key, IntWritable value,
			int numPartitions) {
		if (Integer.parseInt(key.getFirst().toString()) < 51)
			return 0;
		else
			return 1 % numPartitions;
	}
}