package com.an.mrproject.pair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class DataPartitioner extends Partitioner<PairWritable, IntWritable> {

	@Override
	public int getPartition(PairWritable key, IntWritable value, int task) {
		// TODO Auto-generated method stub

		int main = key.getX();

		if (task == 0) {
			return 0;
		}

		if (main < 25) {
			return 0;
		} else {
			return 1;
		}
	}

}
