package com.Spark;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

public class UniversityDataProc {

	private static int pagecount = 1;
	
	public static void main(String[] args) {
		String pageIdTitleInput = args[0];
		String universitypath = args[1];
		String outputpath = args[2];
		WikiPreprocessing(pageIdTitleInput, universitypath, outputpath);
	}
	
	private static void WikiPreprocessing(String pageIdTitleInput, String universitypath, String outputpath)
	{
		BufferedReader br = null;
		FileReader fr = null;
		OutputStream outputStream = null;
		Writer outputStreamWriter = null;
		int counter = 0;

		try {
			fr = new FileReader(universitypath);
			br = new BufferedReader(fr);
			
			HashSet<String> universityNames = new HashSet<String>();
			
		    Map<String, Integer> allTitles = new HashMap<String, Integer>();
			
			String line;
			while ((line = br.readLine()) != null) {
				
				String[] parts = line.split(",");
				
				if(parts.length < 2)
					continue;
				
				universityNames.add(parts[1].trim().toLowerCase());
				
		    	counter++;

			    if(counter%1000==0)
				{
					System.out.println("Getting page " + counter);
				}
			}
		    
			fr = new FileReader(pageIdTitleInput);
			br = new BufferedReader(fr);
		    
			outputStream = new FileOutputStream(outputpath);
		    outputStreamWriter = new OutputStreamWriter(outputStream);
			
		    Map<String, Integer> universityPageIds= new HashMap<String, Integer>();
		    
			while ((line = br.readLine()) != null) {
				
				String[] parts = line.split(",");
				
				String name = parts[1];
			    int pageid = Integer.parseInt(parts[0]);
			    if(universityNames.contains(name))
			    {
			    	universityPageIds.put(name, pageid); 
			    }
			}
			
			int cnter = 1;
			
			for(Map.Entry<String, Integer> page : universityPageIds.entrySet())
			{
				if(cnter == universityPageIds.size())
				{
					outputStreamWriter.write(page.getKey() +","+page.getValue());
				}
				else{
					outputStreamWriter.write(page.getKey() +","+page.getValue()+"\n");
				}

				cnter++;
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
