package org.usergrid.persistence.cassandra;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

public class CassandraClusterFactory {

	private CassandraHostConfigurator cassandraHostConfigurator = null;
	private String clusterName = null;
	
	public CassandraClusterFactory(String clusterName, CassandraHostConfigurator cassandraHostConfigurator) {
		this.clusterName = clusterName;
		this.cassandraHostConfigurator = cassandraHostConfigurator;
	}
	
	public Cluster create() {
		return HFactory.getOrCreateCluster(clusterName, cassandraHostConfigurator);
	}
	
}
