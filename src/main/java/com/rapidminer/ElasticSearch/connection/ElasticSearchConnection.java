package com.rapidminer.ElasticSearch.connection;

import java.net.InetAddress;
import java.util.logging.Logger;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
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
		  		//	Settings settings = Settings.settingsBuilder().put("cluster.name", ElasticSearchConnection.this.getParameter("cluster_name")).build();
			LOGGER.fine("inside try for dowork");
						String host = ElasticSearchConnection.this.getParameter("server_url");
						String port = ElasticSearchConnection.this.getParameter("server_port");
						String clustername = ElasticSearchConnection.this.getParameter("cluster_name");
				//		
						//	LOGGER.finest("I am going to try to load something newwwweeee");
				//			Client client = TransportClient.builder()
				//		        .settings(settings)
				//		        .build()
				//		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
						//	LOGGER.finest("Done building client");
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

