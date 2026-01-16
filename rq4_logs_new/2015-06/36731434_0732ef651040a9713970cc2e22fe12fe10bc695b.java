package com.sap.idm.learning.Commands;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;

public class CopyFile extends PromptOperation {

	private static final int NUMBER_OF_NECESSARY_ARGUMENTS = 2;
	
	public void copyFiles(File sourceFile, File destFile) {
		 FileChannel source = null;
		 FileChannel destination = null;
		 
		 if(isValidFile(sourceFile)){
			 try {	 
				 if(!destFile.exists()) {
					 destFile.createNewFile();
				 }
				  source = new FileInputStream(sourceFile).getChannel();
				  destination = new FileOutputStream(destFile).getChannel();
				  destination.transferFrom(source, 0, source.size());
				  System.out.println(OPERATION_SUCCEEDED);
				 } catch (IOException e) {
					System.out.println(OPERATION_FAILED);
					e.printStackTrace();
				} finally {
					 try {
						   if(source != null) {
						     source.close();
						   }
					    } catch (IOException e) {
						     e.printStackTrace();
					    } 
					 try {
						   if(destination != null) {
							 destination.close();
						    }
						 } catch (IOException e) {
							e.printStackTrace();
						 }
				}
		 } else {
			 System.out.println(FILE_NOT_FOUND_MSG);
		 }
   }

	@Override
    public void executeOperation(List<String> arguments) {
		boolean areArgumentsNumberCorrect = argumentsNumberCheck(NUMBER_OF_NECESSARY_ARGUMENTS, arguments.size());
		if(areArgumentsNumberCorrect) {
			String sourceFile = arguments.get(FIRST_ARGUMENT);
			String destinationFile = arguments.get(SECOND_ARGUMENT);			
			copyFiles(openFile(sourceFile), openFile(destinationFile));
		}
	}
}