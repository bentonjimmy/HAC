package com.clustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class Cluster implements WritableComparable<Cluster> 
{

	private TreeMap<LongWritable, DoubleWritable> links;
	private LongWritable id;
	private boolean active;
	private double vector[];
	private long size;
	private long nearestNode;
	private double shortestDistance;
	
	public Cluster()
	{
		active = true;
		size = 1;
		
	}
	
	/**
	 * Constructor that creates the cluster using the three values passed in the parameters
	 * @param x
	 * @param y
	 * @param z
	 */
	public Cluster(double x, double y, double z)
	{
		super();
		this.vector = new double[] {x, y, z};
	}
	
	/**
	 * Sets the id of the cluster.  If the cluster already has an id and is merging with another cluster than
	 * the id of the cluster is set to the lesser number
	 * @param argID
	 * @return
	 */
	public boolean checkAndSetMinimum(LongWritable argID)
	{
		if(argID != null)
		{
			if(id == null)
			{
				id = new LongWritable(argID.get());
				return true;
			}
			if(argID.get() < id.get())
			{
				id = new LongWritable(argID.get());
				return true;
			}
		}
		return false;
	}
	
	public void addConnection(LongWritable id, DoubleWritable weight)
	{
		if(links == null)
		{
			links = new TreeMap<LongWritable, DoubleWritable>();
		}
		links.put(id, weight);
	}
	
	public DoubleWritable getConnection(LongWritable id)
	{
		if(links != null)
		{
			return links.get(id);
		}
		return null;
	}
	
	public boolean removeConnection(LongWritable id)
	{
		if(links != null)
		{
			links.remove(id);
			return true;
		}
		return false;
	}
	
	public void mergeClusters(Cluster c)
	{
		if(c != null)
		{
			this.size++;
			c.setActive(false);
			System.out.println("Merging "+c.id+" into " + this.id);
		}
	}
	
	public Cluster clone()
	{
		Cluster newCluster = new Cluster();
		newCluster.setActive(active);
		newCluster.setNearestNode(nearestNode);
		newCluster.setShortestDistance(shortestDistance);
		newCluster.setSize(size);
		double[] newvector = new double[vector.length];
		for(int i=0; i<newvector.length; i++)
		{
			newvector[i] = vector[i];
		}
		newCluster.setVector(newvector);
		newCluster.checkAndSetMinimum(id);
		if(links != null)
		{
			Set<LongWritable> keys = links.keySet(); //get all keys
			for(LongWritable lw : keys)
			{
				LongWritable key = new LongWritable(lw.get());
				DoubleWritable value = new DoubleWritable(links.get(lw).get());
				newCluster.addConnection(key, value);
			}
		}
		return newCluster;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		id = new LongWritable();
		id.readFields(in);
		active = in.readBoolean();
		int vectorLength = in.readInt();
		vector = new double[vectorLength];
		for(int i=0; i< vectorLength; i++)
		{
			vector[i] = in.readDouble();
		}
		size = in.readLong();
		nearestNode = in.readLong();
		shortestDistance = in.readDouble();
		int length = in.readInt();
		if(length > -1) //test if there are links to read
		{
			links = new TreeMap<LongWritable, DoubleWritable>();
			for(int i=0; i<length; i++)
			{
				LongWritable key = new LongWritable();
				key.readFields(in);
				DoubleWritable value = new DoubleWritable();
				value.readFields(in);
				links.put(key, value);
			}
		}

	}

	@Override
	public void write(DataOutput out) throws IOException 
	{
		id.write(out);
		out.writeBoolean(active);
		out.writeInt(vector.length);
		for(int i=0; i<vector.length; i++)
		{
			out.writeDouble(vector[i]);
		}
		out.writeLong(size);
		out.writeLong(nearestNode);
		out.writeDouble(shortestDistance);
		if(links == null) 
		{
			out.writeInt(-1);
		}
		else
		{
			out.writeInt(links.size());
			Set<LongWritable> keys = links.keySet(); //get all keys
			for(LongWritable lw : keys)
			{
				lw.write(out); //write key
				links.get(lw).write(out); //write the value
			}
		}
	}

	@Override
	public int compareTo(Cluster o) 
	{
		boolean equals = true;
		for(int i=0; i<vector.length; i++)
		{
			if(vector[i] != o.vector[i])
			{
				equals = false;
				break;
			}
		}
		if(equals)
			return 0;
		else
			return 1;
	}
	
	@Override
	public String toString()
	{
		return "Cluster:"+id;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public double[] getVector() {
		return vector;
	}

	public void setVector(double[] vector) {
		this.vector = vector;
	}

	public LongWritable getId() {
		return id;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public long getNearestNode() {
		return nearestNode;
	}

	public void setNearestNode(long nearestNode) {
		this.nearestNode = nearestNode;
	}

	public double getShortestDistance() {
		return shortestDistance;
	}

	public void setShortestDistance(double shortestDistance) {
		this.shortestDistance = shortestDistance;
	}

}
