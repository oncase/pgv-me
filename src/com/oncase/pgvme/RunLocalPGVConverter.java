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

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

/**
 * RunLocalPGVConverter simply executes a .ktr file that lives
 * inside of the JAR bundle thata contains the class.
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
		
		// Create an instance of this class for convenience
		instance = new RunLocalPGVConverter();
		
		// run the transformation from inside of the JAR
		Trans trans = instance.runTransformationFromFileSystem("file_parser.ktr");
		
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
	

	}

	/**
	 * This method gives the InputStream to the referenced transformation
	 * that will be executed.
	 * @author marpontes
	 * @param filename the file contained inside of the jar file with its path from the root
	 * @return the InputStream object that contains the transformation XML metadata
	 */
	private InputStream getIS(String filename) throws IOException{
		String jar = getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
		URL u = new URL("jar:file:" + jar + "!/"+filename);
		InputStream in = u.openStream();
		return in;
	}
	
	/**
	 * This method returns the path where the JAR file is.
	 * This path will be passed down as a parameter to the
	 * transformation.
	 * @author marpontes
	 * @return the path on the filesystem till the JAR file
	 */
	private String getPathParam(){
		
		String path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
		
		path = path.substring(0,path.lastIndexOf("/"));
		
		return path;
		
	}
	
	/**
	 * This method executes a transformation defined in a ktr file
	 * 
	 * It does the following:
	 * 
	 * - Creates the Transformation metadata from a ktr contained in the JAR
	 * - Sets up and passes down to the transMeta, the parameters defined
	 * - Sets up the log level of the transformation
	 * - Executes the transformation, waiting for it to finish
	 * - Examines the result of the transformation
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
			TransMeta transMeta = new TransMeta(getIS(filename),null,false,null,null);
			
			// assign the value to the parameter on the transformation
			
			final String source = getPathParam()+"/";
			final String destination = source+"export/";
			
			
			transMeta.setParameterValue("source",source);
			transMeta.setParameterValue("destination",destination);
			
			System.out.println("Destination --------> "+ destination);
			System.out.println("Source --------> "+ source);
			
			
			
			// Creating a transformation object which is the programmatic representation of a transformation 
			// A transformation object can be executed, report success, etc.
			Trans transformation = new Trans(transMeta);
			
			// adjust the log level
			transformation.setLogLevel(LogLevel.BASIC);

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
