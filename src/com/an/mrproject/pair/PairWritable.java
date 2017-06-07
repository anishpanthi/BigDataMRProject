package com.an.mrproject.pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PairWritable implements Writable {

	private IntWritable x;
	private Text y;

	@Override
	public String toString() {
		return "(" + x + ", " + y + ")";
	}

	public int getX() {
		return x.get();
	}

	public void setX(IntWritable x) {
		this.x = x;
	}

	public String getY() {
		return y.toString();
	}

	public void setY(Text y) {
		this.y = y;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.x.readFields(arg0);
		this.y.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.x.write(arg0);
		this.y.write(arg0);
	}

	public PairWritable(String x, String y) {
		super();
		this.x = new IntWritable(Integer.parseInt(x));
		this.y = new Text(y);
	}

	public PairWritable() {
		super();
	}

	public int compareTo(PairWritable pw) {
		int compareX = this.x.compareTo(pw.x);
		if (compareX == 0) {
			return this.y.compareTo(pw.y);
		}

		return compareX;
	}

}
