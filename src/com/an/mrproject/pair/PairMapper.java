package com.an.mrproject.pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairMapper extends Mapper<Object, Text, PairWritable, IntWritable> {

	Map<String, Map<String, Integer>> combiner = null;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		combiner = new HashMap<>();
	}

	public void map(Object objKey, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String data = line.substring(line.indexOf(" ") + 1, line.length());

		String[] values = data.split("\\s+");

		for (int i = 0; i < values.length - 1; i++) {
			String[] neighbours = getNeighbours(i, values[i], values);

			for (String neighbour : neighbours) {
				if (neighbour == null) {
					break;
				}
				String key = values[i];

				if (combiner.containsKey(key)) {
					Map<String, Integer> items = combiner.get(key);
					if (items.containsKey(neighbour)) {
						items.put(neighbour, items.get(neighbour) + 1);
					} else {
						items.put(neighbour, 1);
					}
				} else {
					Map<String, Integer> items = new HashMap<>();
					items.put(neighbour, 1);
					combiner.put(key, items);
				}
			}
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		Set<String> keys = combiner.keySet();

		for (String key : keys) {
			Map<String, Integer> pairs = combiner.get(key);
			Set<String> values = pairs.keySet();
			int count = 0;

			for (String value : values) {
				count += pairs.get(value);
				context.write(new PairWritable(key, value), new IntWritable(
						pairs.get(value)));
			}
			context.write(new PairWritable(key, "*"), new IntWritable(count));
		}
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
