package com.rapidminer.operator.io;

import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.repository.RepositoryAccessor;

public abstract interface ESParameterProvider {

	
//	 public abstract String getCollectionName()
//			    throws UndefinedParameterError;
			  
//			  public abstract String getConfigurableName()
//			    throws UndefinedParameterError;
			  
//			  public abstract boolean isGenerated()
//			    throws UndefinedParameterError;
			  
			  public abstract RepositoryAccessor getRepositoryAccessor();
}
