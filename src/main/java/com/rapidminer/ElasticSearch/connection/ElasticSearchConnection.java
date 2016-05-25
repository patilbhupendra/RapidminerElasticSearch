package com.rapidminer.ElasticSearch.connection;

import java.awt.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.net.InetAddress;
import java.util.logging.Logger;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.rapidminer.operator.io.ExampleSetToElasticSearchWriter;
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.config.AbstractConfigurable;
import com.rapidminer.tools.config.TestConfigurableAction;
import com.rapidminer.tools.config.actions.ActionResult;
import com.rapidminer.tools.config.actions.SimpleActionResult;

public class ElasticSearchConnection extends AbstractConfigurable {

	
	private static final Logger LOGGER = Logger.getLogger(ElasticSearchConnection.class
            .getName());

	private TestConfigurableAction testAction = null;
	@Override
	public String getTypeId() {
		// TODO Auto-generated method stub
		return "elasticsearch";
	}
	 public TestConfigurableAction getTestAction()
	  {
	    return this.testAction;
	  }
	 
	 public String[] getListofIndexes(Client client)
		{
			MetaData metadata =  client.admin().cluster()
				    .prepareState().execute()
				    .actionGet().getState()
				    .getMetaData();
					
					CreateIndexRequest request = new CreateIndexRequest();
				//	request.
					//client.admin().indices().create(request)
					
					String[] availableIndexes =  metadata.getConcreteAllIndices();
					
					
					
					return availableIndexes;
					
				
					
					/***********************************************************************/
					
		}
	 
	 
	public ElasticSearchConnection()
	{
		LOGGER.fine("Setting up new ES connection");
		 this.testAction = new TestConfigurableAction()
		 {
			
			public ActionResult doWork()
		      {
				  ActionResult.Result result = null;
			        String message = null;
				try
				{
		  		//	
					LOGGER.fine("inside try for dowork");
					String host = ElasticSearchConnection.this.getParameter("server_url");
					String port = ElasticSearchConnection.this.getParameter("server_port");
					String clustername = ElasticSearchConnection.this.getParameter("cluster_name");
				//		
					ElasticSearchClient ESclient = new ElasticSearchClient(host, port, clustername);
					Client client = ESclient.getTransportclient();
							
					result = ActionResult.Result.SUCCESS;
					message = I18N.getGUIMessage("gui.configurator.elasticsearch.testresult.success", new Object[0]);
				}
		
				catch(Exception e)
				{
					result = ActionResult.Result.FAILURE;
					message = I18N.getGUIMessage("gui.configurator.elasticsearch.testresult.failure", new Object[0]);
					LOGGER.fine("Exception");
					LOGGER.fine(e.getMessage());
				}
				return new SimpleActionResult(message, result);
				
		      }//action result
		 };//this.test action

	}//elastic search connection
}//class

