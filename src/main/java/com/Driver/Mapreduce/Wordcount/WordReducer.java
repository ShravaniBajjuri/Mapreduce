package com.Driver.Mapreduce.Wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, IntWritable, Text,IntWritable> {

	 //@Override
protected void reduce(Text key, Iterable<IntWritable> values,
	Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	  int words = 0;
	
	  for (IntWritable occurance : values) {
	
	   words += occurance.get();
	
	  }
	
	  context.write(key,new IntWritable(words));

	 }

}

