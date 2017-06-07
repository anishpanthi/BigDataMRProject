package com.an.mrproject.pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairMapper extends Mapper<Object, Text, PairWritable, IntWritable> {

	Map<String, Map<String, Integer>> combiner = null;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		combiner = new HashMap<>();
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
	}

	private String[] getNeighbours(int index, String item, String[] data) {

		int length = data.length - (index + 1);
		String[] neighbours = new String[length];

		int j = 0;
		for (int i = index + 1; i < data.length; i++) {
			if (data[i].equals(item)) {
				break;
			}
			neighbours[j++] = data[i];
		}

		return neighbours;
	}

}
