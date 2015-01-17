package com.clustering;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ClusterMapper extends Mapper<LongWritable, Cluster, LongWritable, Cluster> 
{
	Cluster[] lastMerged = new Cluster[2];
	//Cluster lastMerged1 = null;
	//Cluster lastMerged2 = null;
	boolean depthOne = false;
	double lwValues[] = new double[4];
	SingleLink singlelink = new SingleLink();
	
	protected void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		if (Integer.parseInt(context.getConfiguration().get("recursion.depth")) == 2)
			depthOne = true;
		else
		{
			Configuration conf = context.getConfiguration();
			Path lwPath = new Path(conf.get("lwpath.path"));
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(lwPath));
			LongWritable key = new LongWritable();
			Cluster value = new Cluster();
			
			int i=0;
			//read in the clusters that were last merged
			while(reader.next(key, value))
			{
				lastMerged[i] = value.clone();
				i++;
			}
			reader.close();
		}
		
	}
	
	protected void map(LongWritable key, Cluster value, Context context) 
			throws IOException, InterruptedException
	{
		if(depthOne == true) //check if it's the first time
		{
			//If it's the first time then the nearest node was already found
			context.write(new LongWritable(-1), value);
		}
		else
		{
			LongWritable id = null;
			LongWritable toRemove = null;
			//NEED TO CHANGE THIS SO THAT THE NEW CLUSTER DOESNT GET THE DISTANCE TO ITSELF
			//update distance between current node and newly formed cluster
			double newDistance = singlelink.getNewDistance(value, lastMerged[0], lastMerged[1]);
			if(lastMerged[0].getId().get() < lastMerged[1].getId().get())
			{
				id = lastMerged[0].getId();
				toRemove = lastMerged[1].getId();
			}
			else
			{
				id = lastMerged[1].getId();
				toRemove = lastMerged[0].getId();
			}
			value.addConnection(id, new DoubleWritable(newDistance));
			value.removeConnection(toRemove);
			//check if the new distance is shorter than the currently held shortest distance
			//and if the node closest to value was the node we are removing
			if(newDistance < value.getShortestDistance() || value.getNearestNode() == toRemove.get())
			{
				value.setShortestDistance(newDistance);
				value.setNearestNode(id.get());
			}
			
			context.write(new LongWritable(-1), value);
		}
	}
}
