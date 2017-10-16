package com.Spark;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

public class TitleNumber {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String inputpath = args[0];
		String outputpath = args[1];
		PageTitleProcessing(inputpath, outputpath);
	}
	
	// Create ID pair between page and linked pages and save it to local file.
	private static void PageTitleProcessing(String inputpath, String outputpath)
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

			String line;

			Map<String, Integer> allTitles = new HashMap<String, Integer>();
			
			int titleId = 1;
			
			while ((line = br.readLine()) != null) {
				
				String[] parts = line.split("\t");
				
			    if(!allTitles.containsKey(parts[0]))
			    {
			    	allTitles.put(parts[0],titleId);
			    	titleId++;
			    }
			}
			
			fr = new FileReader(inputpath);
			br = new BufferedReader(fr);
			
			while ((line = br.readLine()) != null) {
				
				String[] parts = line.split("\t");
				
				if(allTitles.containsKey(parts[1]))
				{
					int keyId = allTitles.get(parts[0]);
					int valueId = allTitles.get(parts[1]);
					outputStreamWriter.write(keyId + "\t" + valueId + "\n");
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
