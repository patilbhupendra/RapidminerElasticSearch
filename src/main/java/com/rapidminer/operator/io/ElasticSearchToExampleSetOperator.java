package com.rapidminer.operator.io;

import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.search.SearchHit;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class ElasticSearchToExampleSetOperator extends AbstractReader<ExampleSet> {

	public ElasticSearchToExampleSetOperator(OperatorDescription description)
		 {
		super(description, ExampleSet.class);
		// TODO Auto-generated constructor stub
	}


	@Override
	public ExampleSet read() throws OperatorException {
		// TODO Auto-generated method stub
		//TODO figure out which is  the correct setting to use
		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", "myClusterName").build();
		
		Client client = TransportClient.builder().settings(settings).build();
		
		QueryBuilder qb = termQuery("multi", "test");
		
		
		SearchResponse scrollResp = client.prepareSearch("index1", "index2")
			//	.addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
		        .setScroll(new TimeValue(60000))
		        .setQuery(qb)
		        .setSize(100).execute().actionGet();
		
		
		//Add transport addresses and do something with the client...
		
		
		// MatchAll on the whole cluster with all default options
	//	SearchResponse scrollResp = client.prepareSearch().execute().actionGet();
		
		while (true) {

		    for (SearchHit hit : scrollResp.getHits().getHits()) {
		        //Handle the hit...
		    
		    }
		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
		    //Break condition: No hits are returned
		    if (scrollResp.getHits().getHits().length == 0) {
		        break;
		    }
		   
		}
		
		
		return null;
	}

	

}
