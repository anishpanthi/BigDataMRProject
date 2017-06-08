package com.an.mrproject.pair;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairReducer extends
		Reducer<PairWritable, IntWritable, PairWritable, DoubleWritable> {

	private int totalFrequency = 0;

	public void reduce(PairWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int sum = sum(values);

		if (key.getSecond().toString().equals("*")) {
			totalFrequency = sum;
		} else {
			context.write(key,
					new DoubleWritable((double) sum / totalFrequency));
		}
	}

	public int sum(Iterable<IntWritable> values) {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		return sum;
	}
}