/*! ******************************************************************************
*
* Pentaho Data Integration
*
* Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*
*******************************************************************************
* @marpontes
* Executes a single transformation file contained in
* a distributed JAR file
******************************************************************************/

package com.oncase.pgvme;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.lang.RandomStringUtils;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

/**
 * This class demonstrates how to load and execute a PDI transformation.
 * It covers loading from both file system and repositories, 
 * as well as setting parameters prior to execution, and evaluating
 * the result.
 */
public class RunLocalPGVConverter {

	
	public static RunLocalPGVConverter instance; 
	
	/**
	 * @param args not used
	 */
	public static void main(String[] args) {

		// Kettle Environment must always be initialized first when using PDI
		// It bootstraps the PDI engine by loading settings, appropriate plugins etc.
		try {
			KettleEnvironment.init();
		} catch (KettleException e) {
			e.printStackTrace();
			return;
		}
		
		// Create an instance of this demo class for convenience
		instance = new RunLocalPGVConverter();
		
		// run a transformation from the file system
		Trans trans = instance.runTransformationFromFileSystem("parametrized_transformation.ktr");
		
		// retrieve logging appender
		LoggingBuffer appender = KettleLogStore.getAppender();
		// retrieve logging lines for job
		String logText = appender.getBuffer(trans.getLogChannelId(), false).toString();

		// report on logged lines
		System.out.println("************************************************************************************************");
		System.out.println("LOG REPORT: Transformation generated the following log lines:\n");
		System.out.println(logText);
		System.out.println("END OF LOG REPORT");
		System.out.println("************************************************************************************************");
	
		
		// run a transformation from the repository
		// NOTE: before running the repository example, you need to make sure that the 
		// repository and transformation exist, and can be accessed by the user and password used
		// uncomment and run after you've got a test repository in place

		// instance.runTransformationFromRepository("test-repository", "/home/joe", "parametrized_transformation", "joe", "password");

	}

	/*
	 * @marpontes
	 * This method gives the InputStream to the referenced transformation
	 * that will be executed.
	 * */
	private InputStream getIS() throws IOException{
		String jar = getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
		URL u = new URL("jar:file:" + jar + "!/parametrized_transformation.ktr");
		InputStream in = u.openStream();
		return in;
	}
	
	/**
	 * This method executes a transformation defined in a ktr file
	 * 
	 * It demonstrates the following:
	 * 
	 * - Loading a transformation definition from a ktr file
	 * - Setting named parameters for the transformation
	 * - Setting the log level of the transformation
	 * - Executing the transformation, waiting for it to finish
	 * - Examining the result of the transformation
	 * 
	 * @param filename the file containing the transformation to execute (ktr file)
	 * @return the transformation that was executed, or null if there was an error
	 */
	public Trans runTransformationFromFileSystem(String filename) {
		
		try {
			System.out.println("***************************************************************************************");
			System.out.println("Attempting to run transformation "+filename+" from file system");
			System.out.println("***************************************************************************************\n");
			// Loading the transformation file from file system into the TransMeta object.
			// The TransMeta object is the programmatic representation of a transformation definition.
			
			/*
			 * @marpontes
			 * Originally, the transmeta is built by receiving a String url
			 * I've changed it to receive a InputStream to meet the requirement
			 * of embedding the ktr file.
			 * */
			TransMeta transMeta = new TransMeta(getIS(),null,false,null,null);
			
			// The next section reports on the declared parameters and sets them to arbitrary values
			// for demonstration purposes
			System.out.println("Attempting to read and set named parameters");
			String[] declaredParameters = transMeta.listParameters();
			for (int i = 0; i < declaredParameters.length; i++) {
				String parameterName = declaredParameters[i];
				
				// determine the parameter description and default values for display purposes
				String description = transMeta.getParameterDescription(parameterName);
				String defaultValue = transMeta.getParameterDefault(parameterName);
				// set the parameter value to an arbitrary string
				String parameterValue =  RandomStringUtils.randomAlphanumeric(10);
				
				String output = "Setting parameter "+parameterName+" to \""+parameterValue+"\" [description: \""+description+"\", default: \""+defaultValue+"\"]";
				System.out.println(output);
				
				// assign the value to the parameter on the transformation
				transMeta.setParameterValue(parameterName, parameterValue);
				
			}
			
			// Creating a transformation object which is the programmatic representation of a transformation 
			// A transformation object can be executed, report success, etc.
			Trans transformation = new Trans(transMeta);
			
			// adjust the log level
			transformation.setLogLevel(LogLevel.MINIMAL);

			System.out.println("\nStarting transformation");
			
			// starting the transformation, which will execute asynchronously
			transformation.execute(new String[0]);
			
			// waiting for the transformation to finish
			transformation.waitUntilFinished();
			
			// retrieve the result object, which captures the success of the transformation
			Result result = transformation.getResult();
			
			// report on the outcome of the transformation
			String outcome = "\nTrans "+ filename +" executed "+(result.getNrErrors() == 0?"successfully":"with "+result.getNrErrors()+" errors");
			System.out.println(outcome);
			
			return transformation;

		} catch (Exception e) {
			
			// something went wrong, just log and return 
			e.printStackTrace();
			return null;
		} 
		
	}
	

}
