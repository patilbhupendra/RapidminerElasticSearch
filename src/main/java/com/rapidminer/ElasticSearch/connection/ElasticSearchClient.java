package com.rapidminer.ElasticSearch.connection;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticSearchClient {

	Client Transportclient = null;
	
	
	public ElasticSearchClient(String serverurl,String portnumber,String cluster_name) throws NumberFormatException, UnknownHostException
	{
		Settings settings = Settings.settingsBuilder().put("cluster.name",cluster_name).build();
		
			Client client = TransportClient.builder()
		        .settings(settings)
		        .build()
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(serverurl),Integer.parseInt(portnumber) ));
			
			this.Transportclient  = client;
	}
	
	public Client getTransportclient()
	{
		return this.Transportclient;
	}
}
