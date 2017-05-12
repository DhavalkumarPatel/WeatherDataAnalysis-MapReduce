package mr.hw1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import mr.hw2.datatypes.SumCountPair;
import mr.hw2.utility.WeatherDataParser;

/**
 * SEQ: Sequential version that calculates the average of the TMAX & TMIN temperatures by station Id. 
 * HashMap is used as an accumulation data structure for grouping the accumulated temperatures and count of records 
 * by station. 
 * @author dspatel
 */
public class Sequential 
{
	private Map<String, SumCountPair> accumulator = new HashMap<>();
	
	/**
	 * main method to execute the sequential program
	 * @param args
	 */
	public static void main(String[] args) 
	{
		long start = System.currentTimeMillis();
		new Sequential().calculateMeanMinAndMaxTemp();
		long end = System.currentTimeMillis();
		long runningTime = end - start;
		
		System.out.println("Running time = " + runningTime);
	}
	
	/**
	 * Calculate the average of the TMAX & TMIN temperatures by station ID, 
	 */
	private void calculateMeanMinAndMaxTemp()
	{
		String inputFileName = "input/1991.csv";
		String outputFileName = "1991.txt";
		
		FileReader fr;
		try 
		{
			fr = new FileReader(inputFileName);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			
			WeatherDataParser parser = new WeatherDataParser();
			
			while ((line = br.readLine()) != null) 
			{
				if(parser.parse(line))
				{
					SumCountPair sumCountPair = new SumCountPair();

					if (parser.isMaxTemp()) 
					{
						sumCountPair.set(parser.getTemperature(), 1, 0, 0);
					} 
					else 
					{
						sumCountPair.set(0, 0, parser.getTemperature(), 1);
					}
					
					sumCountPair.mergeSumCount(accumulator.get(parser.getStationId()));
					accumulator.put(parser.getStationId(), sumCountPair);
				}
			}
			fr.close();
			
			PrintWriter writer = new PrintWriter(new FileOutputStream(outputFileName), true);
			Iterator<Map.Entry<String, SumCountPair>> iterator = accumulator.entrySet().iterator();
		    while (iterator.hasNext()) 
		    {
		        Map.Entry<String, SumCountPair> pair = iterator.next();
		        writer.println(pair.getKey() + ", " + pair.getValue());
		    }
		    writer.close();
		} 
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
}
