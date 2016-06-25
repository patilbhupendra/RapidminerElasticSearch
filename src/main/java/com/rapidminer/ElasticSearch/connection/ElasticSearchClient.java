package com.rapidminer.ElasticSearch.connection;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.ShieldPlugin;

public class ElasticSearchClient {

	Client Transportclient = null;
	private static final Logger LOGGER = Logger.getLogger(ElasticSearchClient.class
			.getName());

	public ElasticSearchClient(ElasticSearchConnection connection) throws NumberFormatException, UnknownHostException
	{
		String serverurl = connection.getParameter("server_url");
		String portnumber = connection.getParameter("server_port");
		String clustername = connection.getParameter("cluster_name");
		String username = connection.getParameter("username");
		String password = connection.getParameter("password");

		Settings settings= null;
		LOGGER.info("uses authentication flag value" + connection.getParameter("uses_authentication"));
		if( connection.getParameter("uses_authentication").equals("false"))
		{
			LOGGER.finest("did not find usernamepassword");
			settings = Settings.settingsBuilder()
					.put("cluster.name",clustername)
					.build();
		}
		else
		{

			settings = Settings.settingsBuilder()
					.put("cluster.name",clustername)
					.put("shield.user",  username + ":" + password )
					//		.put("shield.user", "admin:apple1234")
					.build();
		}

		Client client = TransportClient.builder()
				.addPlugin(ShieldPlugin.class)
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
