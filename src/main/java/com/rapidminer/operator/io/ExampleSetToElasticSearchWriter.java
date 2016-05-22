package com.rapidminer.operator.io;
import java.util.LinkedList;
import java.util.List;
//
//import java.awt.List;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.collections15.IteratorUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.rapidminer.ElasticSearch.connection.ElasticSearchClient;
import com.rapidminer.ElasticSearch.connection.ElasticSearchConnection;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.UserError;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.config.ConfigurationException;
import com.rapidminer.tools.config.ConfigurationManager;
import com.rapidminer.tools.config.ParameterTypeConfigurable;


public class ExampleSetToElasticSearchWriter extends AbstractWriter<ExampleSet> {

	public static final String PARAMETER_CONNECTION = "Connection";
	
	
	  public ExampleSetToElasticSearchWriter(OperatorDescription description)
	  {
		super(description, ExampleSet.class);

	}

	  //TODO how does bulk deal with millions of rows together
	  //convert all to settings and preferences
	  
	private static final Logger LOGGER = Logger.getLogger(ExampleSetToElasticSearchWriter.class
	            .getName());

	@Override
	public ExampleSet write(ExampleSet exampleSet) throws OperatorException {
		
		try
		{
			ElasticSearchConnection connection = null;
			
			
			try
		    {
		      connection = (ElasticSearchConnection)ConfigurationManager.getInstance().lookup("elasticsearch", 
		        getParameterAsString("PARAMETER_CONNECTION"), getProcess().getRepositoryAccessor());
		    }
		    catch (ConfigurationException e)
		    {
		      throw new UserError(this, e, "Couldn't retrieve a Rosette connection.");
		    }
				    String serverUrl = connection.getParameter("server_url");
				    String serverPort = connection.getParameter("server_port");
				    String serverClusterName = connection.getParameter("cluster_name");
				    
				   
				    LOGGER.finest(serverUrl);
				    LOGGER.finest(serverPort);
				    LOGGER.finest(serverClusterName);
			
		
				LOGGER.finest("I am going to try to load something newwwweeee");
				Client client = new ElasticSearchClient(serverUrl, serverPort, serverClusterName).getTransportclient();
				LOGGER.finest("Done building client");
		
				 final Iterator<Attribute> attributes = exampleSet.getAttributes().allAttributes();
				 
				java.util.List<Attribute> attributesList=   IteratorUtils.toList(attributes);
				
				
				
				final Iterator<Example> examples = exampleSet.iterator();
				
				String indexName = "twitter5";
				String indexType = "Tweet";
				BulkRequestBuilder bulkRequest = client.prepareBulk();
				
				while(examples.hasNext())
				{
					LOGGER.finest("new example");
					Example currexample = examples.next();
					Map<String, Object> json = new HashMap<String, Object>();
					for(int i=0;i<attributesList.size();i++)
					{
						Attribute a =  attributesList.get(i);
						LOGGER.finest(a.getName());
						json.put(a.getName(), currexample.getValueAsString(a) );
					}
					bulkRequest.add(client.prepareIndex(indexName, indexType)
							.setSource(json)
					        );
					
				}
				
		BulkResponse bulkResponse = bulkRequest.get();
		
		if (bulkResponse.hasFailures()) {
		    // process failures by iterating through each bulk response item
			LOGGER.info("Failures in bulk request");
			LOGGER.info(bulkResponse.buildFailureMessage());
		}
		
		}
		catch(Exception e)
		{
			LOGGER.info(e.getMessage());
		}
		return null;
	}

	
	public List<ParameterType> getParameterTypes()
	  {
	    List<ParameterType> types = super.getParameterTypes();
	    
	    ParameterType connection = new ParameterTypeConfigurable("PARAMETER_CONNECTION", I18N.getMessage(I18N.getGUIBundle(), "gui.parameter.elasticsearch.connection.message", new Object[0]), "elasticsearch");
	    
	    connection.setOptional(false);
	    connection.setExpert(false);
	    types.add(connection);
	    
//	    types.addAll(this.attrSelector.getParameterTypes());
	    return types;
	  }
	  
}
