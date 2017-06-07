package com.an.mrproject.pair;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairReducer extends
		Reducer<PairWritable, IntWritable, PairWritable, FloatWritable> {

	private int marginalCounter = 0;

	public void reduce(PairWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		if (key.getY().equals("*")) {
			int temp = 0;
			for (IntWritable value : values) {
				temp += value.get();
			}
			marginalCounter = temp;
		} else {
			for (IntWritable value : values) {
				float frequency = (float) value.get() / marginalCounter;
				context.write(key, new FloatWritable(frequency * 100));
			}
		}
	}
}
