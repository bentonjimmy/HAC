package com.clustering;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class ClusterJob 
{
	public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException
	{
		ClusterJob cj = new ClusterJob();
		cj.runJob();
	}
	
	public void runJob() throws IOException,
	InterruptedException, ClassNotFoundException 
	{
		int depth = 1;
		Configuration conf = new Configuration();
		conf.set("recursion.depth", depth + "");
		
		Path in = new Path("/Users/jmb66/Documents/NJIT/GradProject/DataSets/HAC/SimplePoints.txt");
		
		Path clustered = new Path("/Users/jmb66/Documents/NJIT/GradProject/DataSets/HAC/Clustered/clusters.seq");
		Path out = new Path("/Users/jmb66/Documents/NJIT/GradProject/DataSets/HAC/depth_1");
		conf.set("lwpath.path", clustered.toString());
		
		Job job = Job.getInstance(conf, "Clustering");
		
		job.setMapperClass(DataImporter.class);
		job.setReducerClass(DataReducer.class);
		job.setJarByClass(DataImporter.class);
		
		SequenceFileInputFormat.addInputPath(job, in);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out))
			fs.delete(out, true);
		/*
		if(fs.exists(clustered))
			fs.delete(clustered, true);
			*/
		
		SequenceFileOutputFormat.setOutputPath(job, out);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Cluster.class);
		
		job.waitForCompletion(true);
		
		long counter = job.getCounters().findCounter(ClusterReducer.UpdateCounter.UPDATED).getValue();
		depth++;
		while (counter > 0) 
		{
			conf = new Configuration();
			conf.set("recursion.depth", depth + "");
			conf.set("lwpath.path", clustered.toString());
			job = Job.getInstance(conf, "Clustering " + depth);
			
			job.setMapperClass(ClusterMapper.class);
			job.setReducerClass(ClusterReducer.class);
			job.setJarByClass(ClusterMapper.class);
			
			in = new Path("/Users/jmb66/Documents/NJIT/GradProject/DataSets/HAC/depth_" + (depth - 1) + "/");
			out = new Path("/Users/jmb66/Documents/NJIT/GradProject/DataSets/HAC/depth_" + (depth));
			
			FileInputFormat.addInputPath(job, in);
			if (fs.exists(out))
				fs.delete(out, true);
			
			SequenceFileOutputFormat.setOutputPath(job, out);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Cluster.class);
	
			job.waitForCompletion(true);
			depth++;
			counter = job.getCounters().findCounter(ClusterReducer.UpdateCounter.UPDATED).getValue();
		}
	}
}
