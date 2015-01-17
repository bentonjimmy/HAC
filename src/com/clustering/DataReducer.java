package com.clustering;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.clustering.ClusterReducer.UpdateCounter;

public class DataReducer extends Reducer<LongWritable, Cluster, LongWritable, Cluster> 
{

	protected void reduce(LongWritable key, Iterable<Cluster> values, Context context)
			throws IOException, InterruptedException
	{
		ArrayList<Cluster> clusters = new ArrayList<Cluster>();
		for(Cluster c: values)
		{
			clusters.add(c.clone());
		}
		
		for(int i=0; i<clusters.size(); i++)
		{
			Double shortestDistance = Double.MAX_VALUE;
			Long nearestNode = (long) 0;
			for(int j=0; j<clusters.size(); j++)
			{
				//if(c.getConnection(link.getId()) == null) //test if already in the links
				//{
					//measure distance between cpoints and linkpoints
					double[] cpoints = clusters.get(i).getVector();
					double[] linkpoints = clusters.get(j).getVector();
					double distance = 0;
					double sumOfDifferences = 0;
					
					for(int k=0; k<cpoints.length; k++)
					{
						sumOfDifferences += Math.pow(cpoints[k] - linkpoints[k], 2);
					}
					distance = Math.sqrt(sumOfDifferences);
					//Check if the new distance is less than the current shortest AND the clusters are not the same
					if(distance < shortestDistance && clusters.get(i).getId().get() != clusters.get(j).getId().get())
					{
						shortestDistance = distance;
						nearestNode = clusters.get(j).getId().get();
					}
					clusters.get(i).addConnection(clusters.get(j).getId(), new DoubleWritable(distance)); //add connection to cluster i
					//link.addConnection(c.getId(), new DoubleWritable(distance)); //add connection to link
				//}
			}
			clusters.get(i).setShortestDistance(shortestDistance);
			clusters.get(i).setNearestNode(nearestNode);
			context.write(clusters.get(i).getId(), clusters.get(i));
			context.getCounter(UpdateCounter.UPDATED).increment(1);
		}
	}
}
