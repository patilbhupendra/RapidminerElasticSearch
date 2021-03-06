package com.rapidminer.operator.io;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import org.elasticsearch.client.Client;

import com.rapidminer.ElasticSearch.connection.ElasticSearchClient;
import com.rapidminer.ElasticSearch.connection.ElasticSearchConnection;
import com.rapidminer.gui.tools.ResourceAction;
import com.rapidminer.operator.Operator;
import com.rapidminer.parameter.SuggestionProvider;
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.ProgressListener;
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
		    	
		    	
		    	
		    
		    	
		    	
		    			    	
		//		String serverUrl = connection.getParameter("server_url");
		//		String serverPort = connection.getParameter("server_port");
		//		String serverClusterName = connection.getParameter("cluster_name");

				LOGGER.info("got parameters");
				
				
				//Client client = new ElasticSearchClient(serverUrl, serverPort, serverClusterName).getTransportclient();
				Client client = new ElasticSearchClient(connection).getTransportclient();
				
		//		String serverUrl = connection.getParameter("server_url");
		//				String serverPort = connection.getParameter("server_port");
		//				String serverClusterName = connection.getParameter("cluster_name");
		//				String usenrmae = connection.getParameter("username");
		//				String password = connection.getParameter("password");
				
						
				
		      if (this.type == Type.INDEX)
		      {
		    	  LOGGER.info("type is index");
		    	  String[] indexarray = connection.getListofIndexes(client);
		    	  suggestions.addAll( Arrays.asList(indexarray));
		      }
		
		    }
		    catch (Exception e)
		    {
		    	LOGGER.info("caught an exception");
		    	LOGGER.info(e.getMessage());

		    }
		    finally
		    {
		      pl.complete();
		      LOGGER.info("compelted");
		    }
		    return suggestions;
		  }


		@Override
		public ResourceAction getAction() {
			LOGGER.info("getactions");
			// TODO Auto-generated method stub
			return null;
		}

}
