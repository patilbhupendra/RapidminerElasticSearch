package com.rapidminer.ElasticSearch.connection;

import groovy.json.JsonBuilder;
import groovy.json.JsonParserType;

import java.awt.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.net.InetAddress;
import java.util.logging.Logger;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;






import org.elasticsearch.common.xcontent.support.XContentMapValues;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

//import org.codehaus.jackson.map.ObjectMapper;


//import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonAnyFormatVisitor;
//import org.codehaus.jackson.JsonParser;
//import org.codehaus.*;

import com.fasterxml.jackson.databind.ObjectMapper;
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
	 
	 
	 
	 
	
	 
	 public Integer getMappings(Client client, String indexName, String type, String[] fieldsarray)
	 {
		 MetaData metadata =  client.admin().cluster()
				    .prepareState().execute()
				    .actionGet().getState()
				    .getMetaData();
					
		 String[] indices = {indexName};
		 String[] types = {type};
		//  metadata.findMappings(indices, types);
		 // metadata.findMappings(indices,types)
		  //;
		  LOGGER.info("trying new stuff");
		  
		  Map<String, String> TypeAsMap = new HashMap<>();
		  
		  GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(indices).get();  
		  
	        MappingMetaData mappingMetaData = getMappingsResponse.mappings().get(indexName).get(type);
	        
	     
	     
		  
		  try
		  {
			   Map<String, Object> mappingSource = mappingMetaData.sourceAsMap();
				  
				  for(String col: fieldsarray)
				  {
					  try
					  {
					  String key =  "properties." + col;
					  Map aField = ((Map) XContentMapValues.extractValue(key, mappingSource));
					  LOGGER.info("this is type of for column "+ key + "===" + aField.get("type").toString());
					  }
					  catch(Exception e)
					  {
						  LOGGER.info("couldnot find data type for ->" + col);
					  }
				  }
			  
			  
			  
			  LOGGER.info("trying new stuff +++++++ ");
	//	 GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(indexName);
	  //      GetMappingsResponse getMappingsResponse =
	  //              client.execute(GetMappingsAction.INSTANCE, getMappingsRequest).actionGet();
	 //       MappingMetaData md = getMappingsResponse.getMappings().get("properties").get("field");
	        
	        //String abc=  getMappingsResponse.getMappings().get("properties.amount.type");// .get("type").getSourceAsMap().get("_type");
	        
	//		  GetMappingsResponse getMappingsResponse = client.admin().indices().prepareGetMappings(indices).get();  
			  
	 ////       MappingMetaData mappingMetaData = getMappingsResponse.mappings().get(indexName).get(type);
	  //      Map<String, Object> mappingSource = mappingMetaData.sourceAsMap();
	   //     Map aField = ((Map) XContentMapValues.extractValue("properties.amount", mappingSource));
	  //      assertThat(aField.size(), equalTo(2));
	     //   assertThat(aField.get("type").toString(), equalTo("geo_point"));
	        
	       
	        
	//        LOGGER.info("trying new stuff");
		  }catch(Exception e)
		  {
			  LOGGER.info("ERROR IN FIRST TRY");
		  }
	        
		  
		  
		
		  
	//	  ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> indexMappings =  metadata.findMappings(indices,types);
		 // LOGGER.info("find mappings done");
     //     ImmutableOpenMap<String, MappingMetaData> typeMappings = indexMappings.get(indexName);
     //     LOGGER.info("IMMUTABLE open map done + type is " + type);
         // MappingMetaData mapping = typeMappings.get(type);
     //     MappingMetaData mapping = typeMappings.get(type);
         
        //  public static final Type _elasticsearch_type_mapping_map_type = new TypeToken<LinkedHashMap<String, Mapping>>(){}.getType();
      //    LOGGER.info("Got type mapping");
          try
          {
      //  	  LOGGER.info("trying to get mapping as map");

        	
        //	    Map<String, Mapping> mappingAsMap = new HashMap<>();
        	    
        	
        	/*    GetMappingsResponse res = client.admin().indices().getMappings(new GetMappingsRequest().indices(indexName)).get();
        	    ImmutableOpenMap<String, MappingMetaData> mapping2  = res.mappings().get("properties");
        	    for (ObjectObjectCursor<String, MappingMetaData> c : mapping2) {
        	        System.out.println(">>>>>>>" + c.key+" = "+c.value.source());
        	    }*/
        	
        	    
        	//       Object properties =  mapping.sourceAsMap().get("properties");
                
              //    mappingAsMap = (Map<String, Mapping>) gson.fromJson(gson.toJson(properties), _elasticsearch_type_mapping_map_type);
           //       ObjectMapper mapper = new ObjectMapper();
                  
           //       mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
           //       mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
                
                  
                  
                  
                  //mapper.configure(JsonParser.Feature, true);
                  //mapper.configure(DeserializationFeature., state)
                  
            //      mapper.readValue(jp, valueType)
         //         LOGGER.info(properties.toString());
                  
          //        MappingArray mappingarray = mapper.readValue(properties.toString(),MappingArray.class);
                 
                //  properties.
                  
          //        for(Mapping map: mappingarray.getmappingarray() )
          //        {
                	//LOGGER.info(" this is the type" + map.getType());
          //      	  LOGGER.info(" this is the type" + map.getType());
          //        }
          //        
            
            
          }
          catch(Exception e)
          {
        	  LOGGER.info(e.getMessage());
          }
         
          
         // Map<String, Mapping> mappingAsMap = new HashMap<>();
        //  try {
         //     Object properties = mapping.sourceAsMap().get("properties");
        //      mappingAsMap = (Map<String, Mapping>) gson.fromJson(gson.toJson(properties), _elasticsearch_type_mapping_map_type);
        //      return mappingAsMap;
        //  }
		  
		  return 1;
		 
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

