package com.an.mrproject.hybrid;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class HybridMapper extends
		Mapper<LongWritable, Text, PairWritable, IntWritable> {
	private PairWritable pair = new PairWritable();
	private MapWritable map = new MapWritable();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] terms = value.toString().split("\\s+");
		for (int i = 1; i < terms.length; i++) {

			for (int j = i + 1; j < terms.length && !terms[i].equals(terms[j]); j++) {

				pair = new PairWritable(new Text(terms[i]), new Text(terms[j]));
				IntWritable pairValue = map.get(pair) == null ? new IntWritable(
						1) : new IntWritable(
						(((IntWritable) map.get(pair)).get() + 1));

				map.put(pair, pairValue);
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Entry<Writable, Writable> entry : map.entrySet()) {
			PairWritable key = (PairWritable) entry.getKey();
			IntWritable value = (IntWritable) entry.getValue();
			context.write(key, value);
		}
	}
}