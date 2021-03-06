package com.rapidminer.operator.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import com.rapidminer.ElasticSearch.connection.ElasticSearchClient;
import com.rapidminer.ElasticSearch.connection.ElasticSearchConnection;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DoubleArrayDataRow;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.ParameterTypeSuggestion;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.repository.RepositoryAccessor;
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.config.ConfigurationManager;
import com.rapidminer.tools.config.ParameterTypeConfigurable;

//import java.awt.List;

public class ElasticSearchToExampleSetOperator extends
AbstractReader<ExampleSet> implements ESParameterProvider {

	public static final String PARAMETER_CONNECTION = "Connection";
	private static final Logger LOGGER = Logger
			.getLogger(ElasticSearchToExampleSetOperator.class.getName());
//	public static final String INDEX_NAMES = "indexnames";
	public static final String INDEX_TYPES = "indextypes";
	public static final String FIELDS = "fields";
	public static final String SELECT_INDEX = "indexsuggestion";
	
	//public static final String FIELDNAMES = "fieldnames";
	
	public static final String CONDITION_INPUT_EXISTS = "";
	public static final String ROWCOUNT = "numberofrows";

	
	//public static final String SEARCH_NAME = "search_name";
	//public static final String SEARCH_VALUE = "search_value";
	
	public static final String SEARCH_JSON = "search_json";

	
	
	public ElasticSearchToExampleSetOperator(OperatorDescription description) {
		super(description, ExampleSet.class);
	}

	@Override
	public ExampleSet read() throws OperatorException {

		ArrayList<Attribute> attributes = new ArrayList<Attribute>();

		//String indexList = this.getParameterAsString(INDEX_NAMES);
		String fields = this.getParameterAsString("FIELDS");
		LOGGER.info("thses are the fielsds" + fields);
		String indexTypes = this.getParameterAsString("INDEX_TYPES");
		String indexsuggestion = this.getParameterAsString("SELECT_INDEX");
		
		String searchJson = this.getParameterAsString("SEARCH_JSON");
		
		Integer numberofrowsrequested = 1;
		try
		{
		 numberofrowsrequested = this.getParameterAsInt("ROWCOUNT");
		 LOGGER.info("GOT THE roW VALUES");
		}
		catch (Exception e)
		{
			 numberofrowsrequested = Integer.MAX_VALUE;
			 LOGGER.info("exception in max value");
		}
		if( numberofrowsrequested<1)
		{
			 numberofrowsrequested = 10;
		}

	

				
		MemoryExampleTable table = null;

		try {

			ElasticSearchConnection connection = (ElasticSearchConnection) ConfigurationManager
					.getInstance().lookup("elasticsearch",
							getParameterAsString("PARAMETER_CONNECTION"),
							getProcess().getRepositoryAccessor());
			String serverUrl = connection.getParameter("server_url");
			String serverPort = connection.getParameter("server_port");
			String serverClusterName = connection.getParameter("cluster_name");
			
			
			Client client = new ElasticSearchClient(connection).getTransportclient();

			LOGGER.finest("Elastic search Client Built. Now prepariing Search request Builder");
			SearchRequestBuilder srb = client.prepareSearch();
			srb.setIndices(indexsuggestion);
			String[] fieldsarray = {};
			if(fields != null && !fields.isEmpty())
				if (fields.trim().length() > 0) {
					fieldsarray = fields.split(",");
					for (String x : fieldsarray) {
						srb.addField(x);
						//LOGGER.info(" added field"+ x);
					}
				}


			LOGGER.finest("Elastic Search: Added fields parameter to the Search Request Builder");
			
			if(searchJson != null && !searchJson.isEmpty())
					{
					LOGGER.info("Found search Json as =>" + searchJson);
					srb.setQuery(searchJson);
				}
				else
				{
					LOGGER.info("Did not find Found search Json =>");
				}
			LOGGER.finest("Query builder done");

			SearchResponse scrollResp = srb.setScroll(new TimeValue(60000))
					.setSize(100).execute().actionGet();
			
			Map<String, String> fieldTypes = connection.getMappings(client, indexsuggestion, indexTypes, fieldsarray);

			LOGGER.finest("Elastic Search:Have the scroll Response with "
					+ String.valueOf(scrollResp.getHits().totalHits())
					+ " hits ");

			Integer rowcounter = 0;
			do {
				
			
				for (SearchHit hit : scrollResp.getHits().getHits()) {

					Set<Map.Entry<String, SearchHitField>> set = hit
							.getFields().entrySet();
					Iterator<Map.Entry<String, SearchHitField>> iter = set
							.iterator();

					int attcounter = 0;
					double[] values = new double[set.size()];
					while (iter.hasNext() ) {
						SearchHitField field = iter.next().getValue();
						{
						String fieldtype = "string";
							if(fieldTypes.containsKey(field.name()))
							{
								fieldtype = fieldTypes.get(field.name());
							}
							else
							{
								LOGGER.finest("could not find key for > " + field.name());
								fieldtype = "string";
							}
							Attribute attribute;
							switch(fieldtype)
							{
							case "string":
								if (rowcounter == 0)
								{
									attribute = AttributeFactory.createAttribute(field.name(),Ontology.POLYNOMINAL);
									attributes.add(attribute);
									
								}
								values[attcounter] = attributes.get(attcounter).getMapping().mapString(field.getValue().toString());
								break;

							case "double" :
								if (rowcounter == 0){
									attribute = AttributeFactory.createAttribute(field.name(),Ontology.REAL);
									attributes.add(attribute);
								}
								values[attcounter] = field.getValue();
								break;

							case "datetime":
								if (rowcounter == 0){
									attribute = AttributeFactory.createAttribute(field.name(),Ontology.DATE_TIME);
									attributes.add(attribute);
								}
								values[attcounter] = field.getValue(); 
								break;
							default:
								if (rowcounter == 0){
									attribute = AttributeFactory.createAttribute(field.name(),Ontology.POLYNOMINAL);
									attributes.add(attribute);
								}
								values[attcounter] = attributes.get(attcounter).getMapping().mapString(field.getValue().toString());
								break;

							}
						}
						attcounter++;
					}
					if (rowcounter == 0)
					{
						table = new MemoryExampleTable(attributes);
					}
					rowcounter++;
					if((rowcounter <= numberofrowsrequested))
					{
					table.addDataRow(new DoubleArrayDataRow(values));
					}

				}
				scrollResp = client
						.prepareSearchScroll(scrollResp.getScrollId())
						.setScroll(new TimeValue(60000)).execute().actionGet();
			} while (scrollResp.getHits().getHits().length != 0 && (rowcounter <= numberofrowsrequested )); 
			// Zero hits mark the end of the scroll and the while loop.

		} catch (Exception e) {
			LOGGER.info("Error in query processing");
			LOGGER.info(e.getMessage());
			
		}


		return table.createExampleSet();
	}

	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();

		ParameterType connection = new ParameterTypeConfigurable(
				"PARAMETER_CONNECTION", I18N.getMessage(I18N.getGUIBundle(),
						"gui.parameter.elasticsearch.connection.message",
						new Object[0]), "elasticsearch");

		connection.setOptional(false);
		connection.setExpert(false);
		types.add(connection);

	
		//	ParameterTypeString indexname = new ParameterTypeString("INDEX_NAMES",
		//			I18N.getMessage(I18N.getGUIBundle(),
		//					"gui.parameter.elasticsearch.ES2ExampleSet.indexlist",
		//					new Object[0]));
		//	indexname.setOptional(true);
		//	indexname.setExpert(false);
		//	types.add(indexname);
		
		ParameterTypeSuggestion indexsuggestion = new ParameterTypeSuggestion(
				"SELECT_INDEX", "Select an index you want to get the data from",
				new ElasticSearchSuggestionProvider(this,
						ElasticSearchSuggestionProvider.Type.INDEX));
	
		types.add(indexsuggestion);
		
		ParameterTypeString indextypes = new ParameterTypeString("INDEX_TYPES",I18N.getMessage(I18N.getGUIBundle(),"gui.parameter.elasticsearch.ES2ExampleSet.indextype",new Object[0]));
		//ParameterTypeString indextypes = new ParameterTypeString("INDEX_TYPES","Comma Seperated List of Fileds to retrieve "));
		indextypes.setOptional(true);
		indextypes.setExpert(false);
		types.add(indextypes);


	//	ParameterTypeString fields = new ParameterTypeString("FIELDS",I18N.getMessage(I18N.getGUIBundle(),"gui.parameter.elasticsearch.ES2ExampleSet.fields",new Object[0]));
		ParameterTypeString fields = new ParameterTypeString("FIELDS","Enter Comma seperated list of fields to retrieve");
		fields.setOptional(false);
		fields.setExpert(false);
		types.add(fields);
		
		ParameterTypeString searchJSon = new ParameterTypeString("SEARCH_JSON","Enter the JSON of search query");
		searchJSon.setOptional(true);
		searchJSon.setExpert(true);
		types.add(searchJSon);
		
	
		
		ParameterTypeInt numberofrows = new  ParameterTypeInt("ROWCOUNT","Number of rows you want to retrieve",1,Integer.MAX_VALUE);
		numberofrows.setOptional(true);
		numberofrows.setExpert(false);
		types.add(numberofrows);
		
		
		return types;
	}

	public String getConfigurableName() throws UndefinedParameterError {
		return getParameterAsString("Connection");
	}

	@Override
	public RepositoryAccessor getRepositoryAccessor() {
		// TODO Auto-generated method stub
		return getProcess().getRepositoryAccessor();
	}

}
