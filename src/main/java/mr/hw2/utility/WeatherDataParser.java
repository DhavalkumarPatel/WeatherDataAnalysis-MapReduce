package mr.hw2.utility;

/**
 * This class parses the Weather Data records and also checks for the 
 * validity of the record.
 * @author dspatel
 *
 */
public class WeatherDataParser 
{
	private String stationId;
	private int year;
	private double temperature;
	
	// true for TMAX and false for TMIN
	private boolean isMaxTemp;
	
	public String getStationId() 
	{
		return stationId;
	}
	
	public int getYear() 
	{
		return year;
	}
	
	public boolean isMaxTemp() 
	{
		return isMaxTemp;
	}
	
	public double getTemperature() 
	{
		return temperature;
	}
	
	/**
	 * parse the input record and if the record is valid set the object properties
	 * from input record and return true else return false
	 * @param record
	 * @return true for valid input record else false
	 */
	public boolean parse(String record)
	{
		try 
		{
			String[] tokens = record.split(",");

			this.stationId = tokens[0];
			String tempType = tokens[2];
			this.temperature = Double.parseDouble(tokens[3]);
			
			// another approach is to parse the string to date format and get the year from date, this one is easier
			this.year = Integer.parseInt(tokens[1].substring(0, 4));

			//consider TMAX and TMIN temperatures only 
			if(tempType.equalsIgnoreCase("TMAX"))
			{
				this.isMaxTemp = true;
			}
			else if(tempType.equalsIgnoreCase("TMIN")) 
			{
				this.isMaxTemp = false;
			}
			else
			{
				return false;
			}
			
			//check for missing temperature value
			if(this.temperature == -9999)
			{
				return false;
			}
			
			return true;
		} 
		catch (NullPointerException | ArrayIndexOutOfBoundsException | NumberFormatException e) 
		{
			System.err.println("Error while parsing the input record.");
			e.printStackTrace();
			return false;
		}
	}
	
}
