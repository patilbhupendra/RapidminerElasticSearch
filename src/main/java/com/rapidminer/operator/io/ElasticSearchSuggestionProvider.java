package com.rapidminer.operator.io;

import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.client.Client;

import com.rapidminer.ElasticSearch.connection.ElasticSearchClient;
import com.rapidminer.ElasticSearch.connection.ElasticSearchConnection;
import com.rapidminer.operator.Operator;
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.ProgressListener;
import com.rapidminer.tools.config.ConfigurationManager;
import com.rapidminer.gui.tools.ResourceAction;
import com.rapidminer.operator.Operator;
import com.rapidminer.parameter.SuggestionProvider;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.ParameterService;
import com.rapidminer.tools.ProgressListener;
import com.rapidminer.tools.config.ConfigurationException;
import com.rapidminer.tools.config.ConfigurationManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.rapidminer.tools.config.ConfigurationManager;


public class ElasticSearchSuggestionProvider implements SuggestionProvider<String> {
	
	private static final Logger LOGGER = Logger.getLogger(ElasticSearchSuggestionProvider.class.getName());
	
	private ESParameterProvider esParameterProvider;
	  private Type type;
	  
	  public static enum Type
	  {
	    INDEX,  FIELDS;
	    
	    private Type() {}
	  }
	  
	 public ElasticSearchSuggestionProvider(ESParameterProvider esParameterProvider,Type type)
	  {
		 
		LOGGER.info("reached constructor");
		 
		  if (esParameterProvider == null) {
			  LOGGER.info("esparameterprovider empty");
		      throw new IllegalArgumentException(I18N.getErrorMessage("error.solr.argument_not_set", new Object[] { "Solr parameter provider" }));
		    }
		    if (type == null) {
		    	LOGGER.info("tpye is null");
		      throw new IllegalArgumentException(I18N.getErrorMessage("error.solr.argument_not_set"));
		    }
		    this.esParameterProvider = esParameterProvider;
		    this.type = type;
	  }
	 
	 
	 
	 
		public List<String> getSuggestions(Operator op, ProgressListener pl)
		  {
			
			LOGGER.info("Getting suggestions");
			
		    pl.setCompleted(0);
		    List<String> suggestions = new LinkedList();
		    try
		    {
		    
		    	String connectionname = op.getParameter("PARAMETER_CONNECTION");
		    	
		    	LOGGER.info("trying to get connections ==>" +  connectionname);
		    	ElasticSearchConnection connection = (ElasticSearchConnection)ConfigurationManager.getInstance().lookup("elasticsearch", connectionname 
		    			,this.esParameterProvider.getRepositoryAccessor());
		    	
		    	LOGGER.info("got Connections");
		    	
		    	//ElasticSearchConnection connection = (ElasticSearchConnection)ConfigurationManager.getInstance().lookup("elasticsearch", 
				//		getParameterAsString("PARAMETER_CONNECTION"), getProcess().getRepositoryAccessor());
		    	
		    			    	
				String serverUrl = connection.getParameter("server_url");
				String serverPort = connection.getParameter("server_port");
				String serverClusterName = connection.getParameter("cluster_name");

				LOGGER.info("got parameters");
				Client client = new ElasticSearchClient(serverUrl, serverPort, serverClusterName).getTransportclient();
		      if (this.type == Type.INDEX)
		      {
		    	  LOGGER.info("type is index");
		    	  String[] indexarray = connection.getListofIndexes(client);
		    	  suggestions.addAll( Arrays.asList(indexarray));
		      }
		 //     else if (this.type == Type.FIELDS)
		 //     {
		 //       List<String> fields = new LinkedList(solrConnection.getCollectionFields(this.solrParameterProvider.getCollectionName(), this.solrParameterProvider.isGenerated()).keySet());
		//        suggestions.addAll(fields);
		 //     }
		//      else if (this.type == Type.DATE_FIELDS)
		//      {
		//        List<String> fields = new LinkedList();
		//        Map<String, SolrFieldInfo> fieldMap = solrConnection.getCollectionFields(this.solrParameterProvider
		//          .getCollectionName(), this.solrParameterProvider.isGenerated());
		//        for (Map.Entry<String, SolrFieldInfo> fieldEntry : fieldMap.entrySet()) {
		//          if (SolrjTypeUtil.getSolrDateTypes().contains(((SolrFieldInfo)fieldEntry.getValue()).getFieldType())) {
		//            fields.add(fieldEntry.getKey());
		//          }
		//        }
		 //       suggestions.addAll(fields);
		  //    }
		    }
		    catch (Exception e)
		    {
		    	LOGGER.info("caught an exception");
		    	LOGGER.info(e.getMessage());
		   //   if (Boolean.parseBoolean(ParameterService.getParameterValue("rapidminer.general.debugmode"))) {
		   //     LogService.getRoot().log(Level.WARNING, I18N.getErrorMessage("error.solr.server_general", new Object[0]), e);
		   //   }
		    }
		    finally
		    {
		      pl.complete();
		      LOGGER.info("compelted");
		    }
		   // Collections.sort(suggestions);
		    return suggestions;
		  }


		@Override
		public ResourceAction getAction() {
			LOGGER.info("getactions");
			// TODO Auto-generated method stub
			return null;
		}

}
