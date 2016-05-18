package com.rapidminer.operator.io;

import java.awt.List;
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

import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;

import static org.elasticsearch.common.xcontent.XContentFactory.*;


public class ExampleSetToElasticSearchWriter extends AbstractWriter<ExampleSet> {

	  public ExampleSetToElasticSearchWriter(OperatorDescription description)
	  {
		super(description, ExampleSet.class);
		// TODO Auto-generated constructor stub
	}

	private static final Logger LOGGER = Logger.getLogger(ExampleSetToElasticSearchWriter.class
	            .getName());

	@Override
	public ExampleSet write(ExampleSet exampleSet) throws OperatorException {
		
		try
		{
		Settings settings = Settings.settingsBuilder()
		       .put("cluster.name", "my-application").build();
		
		//Client client = TransportClient.builder().settings(settings).build();
			LOGGER.info("I am going to try to load something newwwweeee");
				Client client = TransportClient.builder()
			        .settings(settings)
			        .build()
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
				LOGGER.info("Done building client");
		
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
				
			/*	Map<String, Object> json = new HashMap<String, Object>();
				json.put("user","kimchy");
				json.put("postDate",new Date());
				json.put("message","trying out Elasticsearch");
				
			*/	
			
				
		
			
				
		BulkResponse bulkResponse = bulkRequest.get();
		
		if (bulkResponse.hasFailures()) {
		    // process failures by iterating through each bulk response item
			LOGGER.info("Failures in bulk request");
		}
		
		}
		catch(Exception e)
		{
			LOGGER.info(e.getMessage());
	
		}
		return null;
	}

}
