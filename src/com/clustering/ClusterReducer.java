package com.clustering;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ClusterReducer extends Reducer<LongWritable, Cluster, LongWritable, Cluster> 
{
	public static enum UpdateCounter {
		UPDATED
	}
	
	private Cluster cluster1, cluster2;
	
	protected void reduce(LongWritable key, Iterable<Cluster> values, Context context)
			throws IOException, InterruptedException
	{
		
		Double shortestDistance = Double.MAX_VALUE;
		cluster1 = null;
		cluster2 = null;
		int i = 0;
		
		//Iterate through all clusters
		for(Cluster c : values)
		{
			i++;
			//Get shortest distance for node
			double currentShortest = c.getShortestDistance();
			//Check if it is the shortest so far or matches the shortest so far
			if(currentShortest < shortestDistance)
			{
				shortestDistance = currentShortest;
				if(cluster1 != null)
				{
					context.write(cluster1.getId(), cluster1);
				}
				cluster1 = c.clone();
				if(cluster2 != null)
				{
					context.write(cluster2.getId(), cluster2);
				}
				cluster2 = null;
			}
			else if(currentShortest == shortestDistance && c.getConnection(cluster1.getId()).get() == currentShortest) //matching node
			{
				cluster2 = c.clone();
			}
			else
			{
				context.write(c.getId(), c);
			}
		}
		
		//write two clusters to be merged to file
		
		//merge two nearest nodes
		if(cluster1.getId().get() < cluster2.getId().get())
		{
			cluster1.mergeClusters(cluster2);
			context.write(cluster1.getId(), cluster1);
		}
		else
		{
			cluster2.mergeClusters(cluster1);
			context.write(cluster2.getId(), cluster2);
		}
		if(i >= 2)
		{
			context.getCounter(UpdateCounter.UPDATED).increment(1);
		}
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException 
    {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.getLocal(conf);
		Path lwPath = new Path(conf.get("lwpath.path"));
		SequenceFile.Writer out = SequenceFile.createWriter(conf, Writer.file(lwPath), Writer.keyClass(LongWritable.class),
				Writer.valueClass(Cluster.class));
		
		//output two merged clusters
		out.append(cluster1.getId(), cluster1);
		out.append(cluster2.getId(), cluster2);
		out.close();
    }
}
