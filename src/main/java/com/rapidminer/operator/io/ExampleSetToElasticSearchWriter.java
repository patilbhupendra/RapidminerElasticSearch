package com.rapidminer.operator.io;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.apache.commons.collections15.IteratorUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.*;
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
import com.rapidminer.tools.Ontology;
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

			/***********************************************************************
			 * code to handle mappings 
			 */
			final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
			XContentBuilder mappingBuilder = BuildMapping(client,indexName, attributesList);
			createIndexRequestBuilder.addMapping(indexType, mappingBuilder);
			
			while(examples.hasNext())
			{

				Example currexample = examples.next();
				Map<String, Object> json = new HashMap<String, Object>();
				for(int i=0;i<attributesList.size();i++)
				{
					Attribute a =  attributesList.get(i);
					//json.put(a.getName(), currexample.getValueAsString(a) );
					
					switch (a.getValueType())
					{
					case Ontology.DATE :
					case Ontology.DATE_TIME : 
						json.put(a.getName(), currexample.getDateValue(a) );
						break ;
					case Ontology.BINOMINAL:
						json.put(a.getName(), currexample.getNominalValue(a));
						break;
					case Ontology.POLYNOMINAL:
					case Ontology.NOMINAL :
						json.put(a.getName(), currexample.getNominalValue(a));
						break;
					case Ontology.NUMERICAL:
						json.put(a.getName(), currexample.getNumericalValue(a));
						break;
					case Ontology.INTEGER :
						json.put(a.getName(), currexample.getNumericalValue(a));
						break;
					case Ontology.REAL:
						json.put(a.getName(), currexample.getNumericalValue(a));
						break;
					case Ontology.STRING:
						json.put(a.getName(), currexample.getNominalValue(a));
					case Ontology.TIME:
						json.put(a.getName(), currexample.getDateValue(a) );
						default:
							json.put(a.getName(), currexample.getNominalValue(a));
							
					}
					
					
					
					
				}
				LOGGER.finest(json.toString());
				bulkRequest.add(client.prepareIndex(indexName, indexType).setSource(json));
				  
								 
				//  IndexRequestBuilder irb = client.prepareIndex().
			}//while
			LOGGER.finest("Attempting to create a Bulk Request");
			BulkResponse bulkResponse = bulkRequest.get();
			LOGGER.finest("Created a Bulk Request");

			if (bulkResponse.hasFailures())
			{
				// process failures by iterating through each bulk response item
				LOGGER.info("Failures in bulk request");
				LOGGER.info(bulkResponse.buildFailureMessage());
			}

		}//try
		catch(Exception e)
		{
			LOGGER.info("Exception in writing");
			LOGGER.info(e.getMessage());
			//LOGGER.info(e.);
		}
		return null;
	}//write

	public XContentBuilder BuildMapping(Client client, String indexName,java.util.List<Attribute> attributesList)
	{
		XContentBuilder mappingBuilder;
		try
		{
		
		LOGGER.finest("Done building CreateIndexRequestBuilder");
		  mappingBuilder = jsonBuilder().startObject().startObject("mappings").startObject("properties");
			LOGGER.finest("Done XContentBuilder");
		for(Attribute att :attributesList)
		{
			LOGGER.finest("Adding type for " + att.getName());
			//final XContentBuilder mappingBuilder = jsonBuilder().startObject().startObject(documentType)
			 //mappingBuilder = 
			String valuetype = "string";
			
			switch (att.getValueType())
			{
			case Ontology.DATE :
			case Ontology.DATE_TIME : 
				valuetype= "date";
				break ;
			case Ontology.BINOMINAL:
				valuetype = "boolean";
				break;
			case Ontology.POLYNOMINAL:
			case Ontology.NOMINAL :
				valuetype = "string";
				break;
			case Ontology.NUMERICAL:
				valuetype = "double";
				break;
			case Ontology.INTEGER :
				valuetype= "long";
				break;
			case Ontology.REAL:
				valuetype = "double";
				break;
			case Ontology.STRING:
				valuetype = "string";
			case Ontology.TIME:
				valuetype = "date";
				default:
					valuetype = "string";
					
			}
				 mappingBuilder.startObject(att.getName()).field("type", att.getValueType()).endObject();
					 
			 LOGGER.finest("DoneAdding type for " + att.getName());
		 
	     	
		}
				mappingBuilder.endObject().endObject().endObject();
		}
		catch(Exception e)
		{
			LOGGER.info("Exception in building maps");
			 mappingBuilder = null;
		}
				return mappingBuilder;
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
