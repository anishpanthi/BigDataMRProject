package com.an.mrproject.pair;

import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairMapper extends Mapper<Object, Text, PairWritable, IntWritable> {

	HashMap<String, HashMap<String, Integer>> combiner = null;

}
