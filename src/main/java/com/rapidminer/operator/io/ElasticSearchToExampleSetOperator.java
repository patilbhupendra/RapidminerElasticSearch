package com.rapidminer.operator.io;
import java.util.LinkedList;
import java.util.List;
import java.net.InetAddress;
import java.util.logging.Logger;

import com.rapidminer.ElasticSearch.connection.ElasticSearchClient;
import com.rapidminer.ElasticSearch.connection.ElasticSearchConnection;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import static org.elasticsearch.index.query.QueryBuilders.*;

//import java.awt.List;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DoubleArrayDataRow;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeAttribute;
import com.rapidminer.parameter.ParameterTypeList;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.config.ConfigurationManager;
import com.rapidminer.tools.config.ParameterTypeConfigurable;


public class ElasticSearchToExampleSetOperator extends AbstractReader<ExampleSet> {

	public static final String PARAMETER_CONNECTION = "Connection";
	private static final Logger LOGGER = Logger.getLogger(ElasticSearchToExampleSetOperator.class
            .getName());

	public static final String INDEX_NAMES = "indexnames";
//	public static final String INDEX_TYPE = "indextype";
	public static final String FIELDS = "fields";
	
	public ElasticSearchToExampleSetOperator(OperatorDescription description)
		 {
		super(description, ExampleSet.class);
		// TODO Auto-generated constructor stub
	}


	@Override
	public ExampleSet read() throws OperatorException {
	
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		
	    String indexList  = this.getParameterAsString("INDEX_NAMES");
	//    String indextypes  = this.getParameterAsString("INDEX_TYPE");

		String fields = this.getParameterAsString("FIELDS");
		
		
		
		if(!(fields.equals(null)))
			if(fields.trim().length()>0)
			{
				String[] fieldsarray = fields.split(",");
				Integer counter = 0;
				for(String x : fieldsarray)
				{
				//	Attribute attribute = AttributeFactory.createAttribute(x, Ontology.POLYNOMINAL);
					
				//	attributes.add(counter,attribute);
				//	counter++;
					
				}
			}
		
		
		
		
		 MemoryExampleTable table = null; 
		//	for(Attribute a : table.getAttributes())
		//	{
		//		LOGGER.info(a.getName());
		//	}
			
		try
		{
		//TODO How does this work on cluster rater than one	
			
			ElasticSearchConnection connection = (ElasticSearchConnection)ConfigurationManager.getInstance().lookup("elasticsearch", 
					getParameterAsString("PARAMETER_CONNECTION"), getProcess().getRepositoryAccessor());
			String serverUrl = connection.getParameter("server_url");
		    String serverPort = connection.getParameter("server_port");
		    String serverClusterName = connection.getParameter("cluster_name");
			
			
			Client client = new ElasticSearchClient(serverUrl, serverPort, serverClusterName).getTransportclient();
			LOGGER.finest("Done building client");
			SearchRequestBuilder srb = client.prepareSearch();
			LOGGER.info(srb.toString());
			if(!(indexList.equals(null)))
			if(indexList.trim().length()>0)
			{
			//	   String FinalListofIndex = "";
				    if(indexList.trim().length()>0)
				    {
				    	String[]  indexlistarray =  indexList.split(",");
					    for(String x : indexlistarray)
					    {
					    	srb.setIndices(x);
					    }
				    }
			}
			LOGGER.finest("DONE ADDING INDEX LIST");
	
			/*
			if(!(indextypes.equals(null)))
			if(indextypes.trim().length()>0)
			    {
				String[] indextypearray =  indextypes.split(",");
					    for(String x : indextypearray)
					    {
					    	//LOGGER.info(srb.toString());
					    }
			    }
			    */
			
			LOGGER.finest("DONE ADDING INDEX TYPES");
			
			if(!(fields.equals(null)))
			if(fields.trim().length()>0)
			{
				String[] fieldsarray = fields.split(",");
				for(String x : fieldsarray)
				{
					 srb.addField(x);
				}
			}
		//	MemoryExampleTable table = new MemoryExampleTable(attributes);
			
			LOGGER.finest("DONE ADDING FIELDS");
		//	QueryBuilder qb = termQuery("Text","event");
			
			LOGGER.finest("Query builder done");
			
			SearchResponse scrollResp = srb
					.setScroll(new TimeValue(60000))
					.setSize(100).execute().actionGet();
			
	
			LOGGER.finest("Have the scroll Response with " +  String.valueOf(scrollResp.getHits().totalHits()) + " hits ");
		
		//Add transport addresses and do something with the client...
		// MatchAll on the whole cluster with all default options
	    //Scroll until no hits are returned
			Integer rowcounter = 0;
	    do {
	        for (SearchHit hit : scrollResp.getHits().getHits()) {
	            //Handle the hit...
	        	
	        	
	      	
	        	Set<Map.Entry<String, SearchHitField>> set = hit.getFields().entrySet();
	        	LOGGER.finest("Size is ");
	        	LOGGER.finest(String.valueOf(set.size()));
                Iterator<Map.Entry<String, SearchHitField>> iter = set.iterator();
                
                
                int attcounter = 0;
                double[] values = new double[set.size()];
                while (iter.hasNext()) 
                {
                    SearchHitField field = iter.next().getValue();
                    LOGGER.finest(field.getValue().toString());
     
                    if(rowcounter==0)
    	        	{
                    Attribute attribute = AttributeFactory.createAttribute(field.name(), Ontology.POLYNOMINAL);
                    attributes.add(attribute);
                	table = new MemoryExampleTable(attributes);
    	        	}
                    
                    values[attcounter] = attributes.get(attcounter).getMapping().mapString(field.getValue().toString());
					attcounter++;
                
                }
                rowcounter++;
                table.addDataRow(new DoubleArrayDataRow(values));
                
	        }
	        scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	    } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
		
		}
		catch(Exception e)
		{
			LOGGER.info("Error in query processing");
			LOGGER.finest(e.getMessage());
		}
		return table.createExampleSet();
	}

	public List<ParameterType> getParameterTypes()
	  {
	    List<ParameterType> types = super.getParameterTypes();
	    
	    ParameterType connection = new ParameterTypeConfigurable("PARAMETER_CONNECTION", I18N.getMessage(I18N.getGUIBundle(), "gui.parameter.elasticsearch.connection.message", new Object[0]), "elasticsearch");
	    
	    connection.setOptional(false);
	    connection.setExpert(false);
	    types.add(connection);
	    
	    ParameterTypeString indexname = new ParameterTypeString("INDEX_NAMES", I18N.getMessage(I18N.getGUIBundle(), "gui.parameter.elasticsearch.ES2ExampleSet.indexlist", new Object[0]));
	    indexname.setOptional(true);
	    indexname.setExpert(false);
	    types.add(indexname);
	    
	 //   ParameterTypeString indextype = new ParameterTypeString("INDEX_TYPE", I18N.getMessage(I18N.getGUIBundle(), "gui.parameter.elasticsearch.ES2ExampleSet.indextype", new Object[0]));
	 //   indextype.setOptional(true);
	 //   indextype.setExpert(false);
	 //   types.add(indextype);
	    
	    
	    ParameterTypeString fields = new ParameterTypeString("FIELDS", I18N.getMessage(I18N.getGUIBundle(), "gui.parameter.elasticsearch.ES2ExampleSet.fields", new Object[0]));
	    fields.setOptional(true);
	    fields.setExpert(false);
	    types.add(fields);
	    

	 
	   
	    
	    return types;
	  }
}
