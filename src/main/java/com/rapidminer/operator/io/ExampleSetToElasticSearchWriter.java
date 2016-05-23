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
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.config.ConfigurationException;
import com.rapidminer.tools.config.ConfigurationManager;
import com.rapidminer.tools.config.ParameterTypeConfigurable;


public class ExampleSetToElasticSearchWriter extends AbstractWriter<ExampleSet> {

	public static final String PARAMETER_CONNECTION = "Connection";
	public static final String INDEX_NAME = "indexname";
	public static final String INDEX_TYPE = "indextype";
	
	
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
			    	LOGGER.info("Error in retreiving Connection parameter");
			    	throw new UserError(this, e, "Couldn't retrieve a Elastic Search connection.");
			    }
			    String serverUrl = connection.getParameter("server_url");
			    String serverPort = connection.getParameter("server_port");
			    String serverClusterName = connection.getParameter("cluster_name");
			    String indexName = this.getParameterAsString("INDEX_NAME");
				String indexType = this.getParameterAsString("INDEX_TYPE");
				
				LOGGER.finest("index name is "  + indexName);
				LOGGER.finest("index type is "  + indexType);

			    LOGGER.finest("Attempting to create Elastic Search Client on server \"" + serverUrl  + "\" on port \"" + serverPort + "\" Cluster is  \""  + serverClusterName +"\"");
				Client client = new ElasticSearchClient(serverUrl, serverPort, serverClusterName).getTransportclient();
				LOGGER.finest("Done building Elastic Search client");
		
				final Iterator<Attribute> attributes = exampleSet.getAttributes().allAttributes();
				java.util.List<Attribute> attributesList=   IteratorUtils.toList(attributes);
				final Iterator<Example> examples = exampleSet.iterator();
				BulkRequestBuilder bulkRequest = client.prepareBulk();
				
				while(examples.hasNext())
				{

					Example currexample = examples.next();
					Map<String, Object> json = new HashMap<String, Object>();
					for(int i=0;i<attributesList.size();i++)
					{
						Attribute a =  attributesList.get(i);
						json.put(a.getName(), currexample.getValueAsString(a) );
					}
					LOGGER.finest(json.toString());
				
					bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(json));
					
				}
				LOGGER.info("Attempting to create a Bulk Request");
				BulkResponse bulkResponse = bulkRequest.get();
				LOGGER.info("Created a Bulk Request");
				
				if (bulkResponse.hasFailures()) {
				    // process failures by iterating through each bulk response item
					LOGGER.info("Failures in bulk request");
					LOGGER.info(bulkResponse.buildFailureMessage());
				}
		
		}
		catch(Exception e)
		{
			LOGGER.info("Exception in writing");
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
	    
	    ParameterTypeString indexname = new ParameterTypeString("INDEX_NAME", I18N.getMessage(I18N.getGUIBundle(), "gui.parameter.elasticsearch.connection.indexname", new Object[0]));
	    indexname.setOptional(false);
	    indexname.setExpert(false);
	    types.add(indexname);
	    
	    ParameterTypeString indextype = new ParameterTypeString("INDEX_TYPE", I18N.getMessage(I18N.getGUIBundle(), "gui.parameter.elasticsearch.connection.indexTYPE", new Object[0]));
	    indextype.setOptional(false);
	    indextype.setExpert(false);
	    types.add(indextype);

	    return types;
	  }
	  
}
