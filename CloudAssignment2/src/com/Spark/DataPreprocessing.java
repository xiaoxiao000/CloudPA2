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

public class DataPreprocessing {

	private static int pagecount = 1;
	
	public static void main(String[] args) {
		String inputpath = args[0];
		String outputpath1 = args[1];
		String outputpath2 = args[2];
		WikiPreprocessing(inputpath, outputpath1, outputpath2);
	}
	
	private static void WikiPreprocessing(String inputpath, String outputpath1, String outputpath2)
	{
		BufferedReader br = null;
		FileReader fr = null;
		OutputStream outputStream = null;
		Writer outputStreamWriter = null;
		int counter = 0;

		try {
			fr = new FileReader(inputpath);
			br = new BufferedReader(fr);
		    Map<String, Integer> allTitles = new HashMap<String, Integer>();
			
			String line;
			while ((line = br.readLine()) != null) {
				
				String[] parts = line.split(",");
				
				if(parts.length < 2)
					continue;
				
				int pageId = Integer.parseInt(parts[0]);
			    String pageTitle = parts[1].trim().toLowerCase();
			    
		    	allTitles.put(pageTitle, pageId);
		    	counter++;

			    if(counter%1000==0)
				{
					System.out.println("Getting page " + counter);
				}
			}
			
			outputStream = new FileOutputStream(outputpath1);
			outputStreamWriter = new OutputStreamWriter(outputStream);
			
			int titleIndex = 1;
			
			for (Map.Entry<String, Integer> entry : allTitles.entrySet()) {
				if(titleIndex == allTitles.size())
				{
					outputStreamWriter.write(entry.getValue() + "," + entry.getKey());
				}else
				{
					outputStreamWriter.write(entry.getValue() + "," + entry.getKey()+"\n");
				}
				titleIndex++;	
			}
		
			System.out.println("PageIdPageTitle.txt saved");
			
		    
			fr = new FileReader(inputpath);
			br = new BufferedReader(fr);
		    
			outputStream = new FileOutputStream(outputpath2);
		    outputStreamWriter = new OutputStreamWriter(outputStream);
		    
			String article;
			
			while ((article = br.readLine()) != null) {
				
				ArrayList<String> pairs = XmlParser(article, allTitles);
				
			    for(String pair: pairs)
			    {
			    	outputStreamWriter.write(pair + "\n");
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
	
	  public static ArrayList<String> XmlParser(String article, Map<String, Integer> allTitles)
	  {
	    String[] parts = article.split("\t");
	    
	    int pageId = Integer.parseInt(parts[0]);
	    String pageTitle = parts[1].trim().toLowerCase();
	    String xmlfile = parts[3];
		ArrayList<String> titleLinks = new ArrayList<String>();
		
		if(pagecount%1000 == 0)
		{
			System.out.println("Processing file no.  " + pagecount +" " + pageTitle);
		}
		
		pagecount++;
		HashSet<String> otherTitles = new HashSet<String>();
		
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(new ByteArrayInputStream(xmlfile.getBytes()));
			doc.getDocumentElement().normalize();

			NodeList nodeList = doc.getElementsByTagName("target");
			
	        for(int x=0,size= nodeList.getLength(); x<size; x++) {
	        	Node node = nodeList.item(x);
	        	
	        	String otherTitle = node.getTextContent().trim().toLowerCase();
	        	
	        	if(otherTitle != null && !otherTitle.equals("") &&!otherTitle.equals(pageTitle))
	        	{
	        		if(allTitles.containsKey(otherTitle))
	        		{       			
	        			String titlelink = pageId +"\t" + allTitles.get(otherTitle); 		
		        		titleLinks.add(titlelink);
		        		otherTitles.add(otherTitle);
	        		}
	        		
	        	}
	        }	
		}catch(Exception ex)
		{	
		}
		
		System.out.println("Total pages processed " + pagecount); 

		return titleLinks;
	  }
	 
}
