package com.an.mrproject.hybrid;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class HybridReducer extends Reducer<PairWritable, IntWritable, Text, Text> {
	MapWritable map = new MapWritable();
	int marginal = 0;
	String currentTerm = null;

	@Override
	protected void reduce(PairWritable pair, Iterable<IntWritable> values,
			Reducer<PairWritable, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {

		Text key = pair.getFirst();
		Text value = pair.getSecond();
		StringBuilder stringBuilder = new StringBuilder();
		DecimalFormat decimalFormat = new DecimalFormat();
		int partialSum = 0;

		for (IntWritable pairCount : values) {
			partialSum += pairCount.get();
		}
		if (currentTerm == null) {
			currentTerm = key.toString();
		}

		if (currentTerm.toString().equals(key.toString())) {
			marginal += partialSum;
			IntWritable individualCount = (IntWritable) map.get(value);
			if (map.containsKey(value)) {
				individualCount.set(individualCount.get() + 1);
			} else {
				map.put(new Text(value.toString()), new IntWritable(partialSum));
			}
		} else if (!currentTerm.toString().equals(key.toString())) {
			emitIt(map, stringBuilder, decimalFormat);
			context.write(new Text(currentTerm),
					new Text("[" + stringBuilder.toString() + "]"));
			currentTerm = key.toString();
			marginal = partialSum;
			map = new MapWritable();
			map.put(new Text(value.toString()), new IntWritable(partialSum));
		}
	}

	private void emitIt(MapWritable map, StringBuilder stringBuilder,
			DecimalFormat decimalFormat) {
		for (Entry<Writable, Writable> entry : map.entrySet()) {
			stringBuilder.append("{");
			stringBuilder.append(entry.getKey().toString());
			stringBuilder.append(",");
			stringBuilder.append(decimalFormat.format(Integer.parseInt(entry
					.getValue().toString()) / (double) marginal));
			stringBuilder.append("}");
		}
	}

	@Override
	protected void cleanup(
			Reducer<PairWritable, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {

		StringBuilder stringBuilder = new StringBuilder();
		DecimalFormat decimalFormat = new DecimalFormat();
		emitIt(map, stringBuilder, decimalFormat);
		context.write(new Text(currentTerm),
				new Text("[" + stringBuilder.toString() + "]"));

	}
}