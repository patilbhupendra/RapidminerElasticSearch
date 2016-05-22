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
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.config.ConfigurationManager;
import com.rapidminer.tools.config.ParameterTypeConfigurable;


public class ElasticSearchToExampleSetOperator extends AbstractReader<ExampleSet> {

	public static final String PARAMETER_CONNECTION = "Connection";
	private static final Logger LOGGER = Logger.getLogger(ElasticSearchToExampleSetOperator.class
            .getName());

	public ElasticSearchToExampleSetOperator(OperatorDescription description)
		 {
		super(description, ExampleSet.class);
		// TODO Auto-generated constructor stub
	}


	@Override
	public ExampleSet read() throws OperatorException {
		
		ArrayList<Attribute> attributes = new ArrayList<Attribute>();
		Attribute dateattribute = AttributeFactory.createAttribute("Text", Ontology.POLYNOMINAL);
		attributes.add(dateattribute);
		MemoryExampleTable table = new MemoryExampleTable(attributes);
		
		
		try
		{
		// TODO Auto-generated method stub
		//TODO figure out which is  the correct setting to use
		//TODO How does this work on cluster rater than one	
			
			ElasticSearchConnection connection = (ElasticSearchConnection)ConfigurationManager.getInstance().lookup("elasticsearch", 
			        getParameterAsString("PARAMETER_CONNECTION"), getProcess().getRepositoryAccessor());
			
			  String serverUrl = connection.getParameter("server_url");
			    String serverPort = connection.getParameter("server_port");
			    String serverClusterName = connection.getParameter("cluster_name");
			
		
		Client client = new ElasticSearchClient(serverUrl, serverPort, serverClusterName).getTransportclient();
		
		
		LOGGER.finest("Done building client");
		
		QueryBuilder qb = termQuery("Text","event");
		
		LOGGER.finest("Query builder done");
		
		SearchResponse scrollResp = client.prepareSearch("twitter5", "twitter")
			//	.addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
		        .setScroll(new TimeValue(60000))
		        .addFields("Text")
		        .setQuery(qb)
		        .setSize(100).execute().actionGet();
		LOGGER.finest("scrollresp");
		
		//Add transport addresses and do something with the client...
		
		
		// MatchAll on the whole cluster with all default options
	    LOGGER.finest(String.valueOf(scrollResp.getHits().totalHits()));
	
	    //Scroll until no hits are returned
	    do {
	        for (SearchHit hit : scrollResp.getHits().getHits()) {
	            //Handle the hit...
	        	LOGGER.finest("searchhit");
		    	
	        	Set<Map.Entry<String, SearchHitField>> set = hit.getFields().entrySet();
	        	LOGGER.finest("Size is ");
	        	LOGGER.finest(String.valueOf(set.size()));
                Iterator<Map.Entry<String, SearchHitField>> iter = set.iterator();
                while (iter.hasNext()) {
                    SearchHitField field = iter.next().getValue();
                    LOGGER.finest(field.getValue().toString());
                    double[] values = new double[1];
					values[0] = attributes.get(0).getMapping().mapString(field.getValue().toString());
					table.addDataRow(new DoubleArrayDataRow(values));
                   
                }
	        }
	        scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	    } while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.
		
		}
		catch(Exception e)
		{
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
	    
//	    types.addAll(this.attrSelector.getParameterTypes());
	    return types;
	  }
}
