package com.rapidminer.ElasticSearch.connection;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import com.rapidminer.tools.I18N;
import com.rapidminer.tools.config.AbstractConfigurable;
import com.rapidminer.tools.config.TestConfigurableAction;
import com.rapidminer.tools.config.actions.ActionResult;
import com.rapidminer.tools.config.actions.SimpleActionResult;
//import org.codehaus.jackson.map.ObjectMapper;
//import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonAnyFormatVisitor;
//import org.codehaus.jackson.JsonParser;
//import org.codehaus.*;

public class ElasticSearchConnection extends AbstractConfigurable {


	private static final Logger LOGGER = Logger.getLogger(ElasticSearchConnection.class
			.getName());

	private TestConfigurableAction testAction = null;
	@Override
	public String getTypeId() {
		return "elasticsearch";
	}
	public TestConfigurableAction getTestAction()
	{
		return this.testAction;
	}

	public Map<String, String> getMappings(Client client, String indexName, String type, String[] fieldsarray)
	{
		MetaData metadata =  client.admin().cluster()
				.prepareState().execute()
				.actionGet().getState()
				.getMetaData();

		LOGGER.finest("Index is" + indexName);
		LOGGER.finest("Type of index is " + type);
		LOGGER.finest("First field is fieldsarray" + fieldsarray[0]);
		
		String[] indices = {indexName};
		String[] types = {type};
		Map<String, String> fieldTypes = new HashMap<>();
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
					LOGGER.finest("this is type of for column "+ key + "===" + aField.get("type").toString());
					fieldTypes.put(col, aField.get("type").toString());
				}
				catch(Exception e)
				{
					LOGGER.info("couldnot find data type for ->" + col);
				}
			}
		}catch(Exception e)
		{
			LOGGER.info("ERROR IN FIRST TRY");
		}
		return fieldTypes;

	}

	public String[] getListofIndexes(Client client)
	{
		MetaData metadata =  client.admin().cluster()
				.prepareState().execute()
				.actionGet().getState()
				.getMetaData();

		CreateIndexRequest request = new CreateIndexRequest();
		String[] availableIndexes =  metadata.getConcreteAllIndices();
		return availableIndexes;
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
					ElasticSearchClient ESclient = new ElasticSearchClient(ElasticSearchConnection.this);
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

