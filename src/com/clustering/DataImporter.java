package com.clustering;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//Assume file with number ID followed by 3 numbers, space delimited
public class DataImporter extends Mapper<LongWritable, Text, LongWritable, Cluster> {

	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		Cluster center = null;
		String values[] = value.toString().split(" ");
		Double parsedValues[] = new Double[values.length - 1];
		int location = 0;
		long id = 0;
		
		for(String s: values)
		{
			if(location == 0)
			{
				//retrieve the cluster's id
				id = Long.parseLong(s);
			}
			else //if(location == 1)//change this
			{
				parsedValues[location - 1] = Double.parseDouble(s);
				
				/*
				center = new Cluster(Double.parseDouble(values[0]), Double.parseDouble(values[1]),
						Double.parseDouble(values[2]));
				center.checkAndSetMinimum(new LongWritable(id));
				
				*/
				
			}
			
			location++;
		}
		center = new Cluster(parsedValues[0], parsedValues[1], parsedValues[2]);
		center.checkAndSetMinimum(new LongWritable(id));
		context.write(new LongWritable(0), center); //use dummy id
	}
}
