package com.Spark;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//To get the page title and its relative id as a pair of records
public class PageTitleIdPair {

	public static void main(String[] args) {
		String inputpath = args[0];
		String outputpath = args[1];
		WikiPreprocessing(inputpath, outputpath);
	}
	
	private static void WikiPreprocessing(String inputpath, String outputpath)
	{
		BufferedReader br = null;
		FileReader fr = null;
		OutputStream outputStream = null;
		Writer outputStreamWriter = null;
		
		try {
			fr = new FileReader(inputpath);
			br = new BufferedReader(fr);
			outputStream = new FileOutputStream(outputpath);
		    outputStreamWriter = new OutputStreamWriter(outputStream);

		    Map<String, Integer> allTitles = new HashMap<String, Integer>();
			int pageId = 1;
			String line;
			while ((line = br.readLine()) != null) {
				
				String[] parts = line.split("\t");
				
				String pageTitle = parts[0];
				
			    if(!allTitles.containsKey(pageTitle))
			    {
			    	allTitles.put(pageTitle,pageId);
			    	outputStreamWriter.write(pageId + "," + pageTitle+"\n");
			    	if(pageId % 10000 == 0)
			    	{
			    		System.out.println("Getting page id =" + pageId);
			    	}
			    	pageId++;
			    }
			}
		
		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

				if(outputStreamWriter != null)
					outputStreamWriter.close();
				
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
}

