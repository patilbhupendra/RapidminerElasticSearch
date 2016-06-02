package com.rapidminer.ElasticSearch.connection;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import com.rapidminer.parameter.ParameterHandler;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.I18N;
import com.rapidminer.tools.config.AbstractConfigurator;

public class ElasticSearchConnectionConfigurator extends
		AbstractConfigurator<ElasticSearchConnection> {

	
	 private static final String I18N_CONF_URL = I18N.getGUIMessage("gui.configurator.elasticsearch.elasticsearch_server_url", new Object[0]);
	 private static final String I18N_CONF_PORT = I18N.getGUIMessage("gui.configurator.elasticsearch.elasticsearch_server_port", new Object[0]);
//	  private static final String I18N_CONF_AUTH = I18N.getGUIMessage("gui.configurator.elasticsearch.http_basic_auth", new Object[0]);
//	  private static final String I18N_CONF_USER = I18N.getGUIMessage("gui.configurator.elasticsearch.http_basic_auth.user", new Object[0]);
//	  private static final String I18N_CONF_PSW = I18N.getGUIMessage("gui.configurator.elasticsearch.http_basic_auth.password", new Object[0]);
	  private static final String I18N_CLUSTER_NAME = I18N.getGUIMessage("gui.configurator.elasticsearch.cluster_name", new Object[0]);
	  
	
	  private static final Logger LOGGER = Logger.getLogger(ElasticSearchConnectionConfigurator.class
	            .getName());

	  
	@Override
	public Class<ElasticSearchConnection> getConfigurableClass() {
		// TODO Auto-generated method stub
		return ElasticSearchConnection.class;
	}

	@Override
	public String getTypeId() {
		// TODO Auto-generated method stub
		return "elasticsearch";
	}

	@Override
	public String getI18NBaseKey() {
		// TODO Auto-generated method stub
		return "elasticsearch";
	}

	@Override
	public List<ParameterType> getParameterTypes(
			ParameterHandler parameterHandler) {
	
		List<ParameterType> parameterTypes = new LinkedList();
	    
	    ParameterTypeString paramServerURL = new ParameterTypeString("server_url", I18N_CONF_URL, false, false);
	    paramServerURL.setDefaultValue("localhost");
	    parameterTypes.add(paramServerURL);
	    
	    
	    ParameterTypeString paramPort = new ParameterTypeString("server_port", I18N_CONF_PORT, false, false);
	    paramPort.setDefaultValue("9300");
	    parameterTypes.add(paramPort);
	    
	    
	/*    parameterTypes.add(new ParameterTypeBoolean("uses_authentication", I18N_CONF_AUTH, false, false));
	    
	    paramTypeString = new ParameterTypeString("user", I18N_CONF_USER, true);
	    paramTypeString.registerDependencyCondition(new BooleanParameterCondition(parameterHandler, "uses_authentication", true, true));
	    
	    parameterTypes.add(paramTypeString);
	    
	    ParameterTypePassword paramTypePassword = new ParameterTypePassword("password", I18N_CONF_PSW);
	    paramTypePassword.registerDependencyCondition(new BooleanParameterCondition(parameterHandler, "uses_authentication", true, true));
	    
	    parameterTypes.add(paramTypePassword);
	    */
	    
	    
	    ParameterTypeString paramClusterName = new ParameterTypeString("cluster_name", I18N_CLUSTER_NAME, false, false);
	    paramClusterName.setDefaultValue("my-application");
	    parameterTypes.add(paramClusterName);
	    
	    
	    return parameterTypes;
		
		
	}

}
