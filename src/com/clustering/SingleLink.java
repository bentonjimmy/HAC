package com.clustering;

public class SingleLink implements ClusteringMethod {

	@Override
	public double getNewDistance(Cluster q, Cluster a, Cluster b) 
	{
		double alphaA, alphaB, beta, gamma;
		
		alphaA = .5;
		alphaB = .5;
		beta = 0;
		gamma = -.5;
		
		return (alphaA * a.getConnection(q.getId()).get()) + (alphaB * b.getConnection(q.getId()).get()) 
				+ (beta * a.getConnection(b.getId()).get()) 
				+ (gamma * Math.abs(a.getConnection(q.getId()).get() - b.getConnection(q.getId()).get()));
	}

}
