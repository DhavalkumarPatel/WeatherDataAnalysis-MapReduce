package mr.hw2.part2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import mr.hw2.datatypes.SumCountPair;
import mr.hw2.datatypes.TextIntPair;
import mr.hw2.utility.WeatherDataParser;

/**
 * TemperatureTimeSeries - This program creates time series of temperature data.
 * It calculates mean minimum temperature and mean maximum temperature, by station, 
 * by year using the secondary sort design pattern to minimize the amount of memory 
 * utilized during Reduce function execution.
 * @author dspatel
 */
public class TemperatureTimeSeries 
{
	/**
	 * Mapper class with only map function
	 * @author dspatel
	 */
	public static class TempratureTimeSeriesMapper extends Mapper<Object, Text, TextIntPair, SumCountPair> 
	{
		private WeatherDataParser parser = new WeatherDataParser();
		private TextIntPair stationYearPair = new TextIntPair();
		private SumCountPair sumCountPair = new SumCountPair();

		/**
		 * map function parses each input record and for each valid record, it emits the stationId & year 
		 * as a TextIntPair (key) and and min/max temperature with count as a SumCountPair object (value).
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			if(parser.parse(value.toString()))
			{
				stationYearPair.set(parser.getStationId(), parser.getYear());

				if (parser.isMaxTemp()) 
				{
					sumCountPair.set(parser.getTemperature(), 1, 0, 0);
				} 
				else
				{
					sumCountPair.set(0, 0, parser.getTemperature(), 1);
				}
				
				context.write(stationYearPair, sumCountPair);
			}
		}
	}
	
	/**
	 * Combiner class with reduce function
	 * @author dspatel
	 */
	public static class TempratureTimeSeriesCombiner extends Reducer<TextIntPair, SumCountPair, TextIntPair, SumCountPair> 
	{
		/**
		 * reduce function gets the TextIntPair (StationId & Year) as a key and iterable list of 
		 * SumCountPairs as values. For each station & year reduce function combines/merges the 
		 * sumCountPairs and emits the same input key with combined sumCountPair object as a value
		 */
		public void reduce(TextIntPair key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException 
		{
			SumCountPair sumCountPair = new SumCountPair();
			
			for (SumCountPair val : values) 
			{
				sumCountPair.mergeSumCount(val);
			}

			context.write(key, sumCountPair);
		}
	}

	/**
	 * Custom partitioner for partitioning by only stationId so that all the records for
	 * same station ends together in same reduce task 
	 * @author dspatel
	 */
	public static class TempratureTimeSeriesPartitioner extends Partitioner<TextIntPair, SumCountPair> 
	{
		@Override
		public int getPartition(TextIntPair key, SumCountPair value, int numPartitions) 
		{
			// multiply by 127 to perform some mixing
			return Math.abs(key.getText().hashCode() * 127) % numPartitions;
		}
	}
	
	/**
	 * 	Custom group comparator for grouping only by stationId so that in one reduce call
	 *  we will have temperature values of all the years of specific station together.
	 * @author dspatel
	 */
	public static class TempratureTimeSeriesGroupComparator extends WritableComparator 
	{
	    protected TempratureTimeSeriesGroupComparator() 
	    {
	      super(TextIntPair.class, true);
	    }
	    
		@Override
		@SuppressWarnings("rawtypes")
	    public int compare(WritableComparable w1, WritableComparable w2) 
	    {
	    	TextIntPair tip1 = (TextIntPair) w1;
	    	TextIntPair tip2 = (TextIntPair) w2;
	    	return tip1.getText().compareTo(tip2.getText());
	    }
	  }
	
	/**
	 * Reducer class with only reduce function
	 * @author dspatel
	 */
	public static class TempratureTimeSeriesReducer extends Reducer<TextIntPair, SumCountPair, Text, NullWritable> 
	{
		/**
		 * reduce function gets the TextIntPair (StationId & Year) as a key and iterable list of 
		 * SumCountPairs as values. Reduce function calculates mean minimum and mean maximum temperatures
		 * for each year of the specific station and emits as a Text in below format:
		 * StationIdn, [(y1, MeanMinn1, MeanMaxn1), (y2, MeanMinn2, MeanMaxn2) ... (y ...)] 
		 * If any stationId does not have any data for specific year then it does not print output
		 * for that specific year i.e. (year, NULL, NULL) as it is not meaningful and its an overhead.
		 */
		public void reduce(TextIntPair key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException 
		{
			String resultStr = key.getText() + ", [";
			SumCountPair sumCountPair = new SumCountPair();
			int currentYear = key.getInteger().get();
			
			for (SumCountPair val : values) 
			{
				if(currentYear == key.getInteger().get())
				{
					// Year is same as previous record so only merge the val to current sumCountPair object 
					sumCountPair.mergeSumCount(val);
				}
				else
				{
					// Year is changed from previous record so calculate the mean temperatures for the previous records
					resultStr += "(" + currentYear + ", " + sumCountPair.toString() + "), ";
					
					// Change the Year and start a new iteration for it
					currentYear = key.getInteger().get();
					sumCountPair.set(val.getMaxTempSum().get(), val.getMaxTempCount().get(), 
							val.getMinTempSum().get(), val.getMinTempCount().get()); 				}
			}

			// Append the mean temperatures of the last year
			resultStr += "(" + currentYear + ", " + sumCountPair.toString() + ")]";
			
			// emit the resultStr as Text
			context.write(new Text(resultStr), NullWritable.get());
		}
	}
	
	/**
	 * main method to execute the Climate Analysis Temperature Time Series MapReduce Job
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		if (args.length != 2) 
		{
			System.err.println("Usage: TemperatureTimeSeries <input path> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		BasicConfigurator.configure();

		Job job = Job.getInstance(conf, "Climate Analysis Temperature Time Series");
		job.setJarByClass(TemperatureTimeSeries.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// set Mapper, Combiner, Partitioner, GroupComparator and Reducer classes in job
		// No need of KeyComparator as it is replaced by compareTo method of Key(TextIntPair) class
		job.setMapperClass(TempratureTimeSeriesMapper.class);
		job.setCombinerClass(TempratureTimeSeriesCombiner.class);
		job.setPartitionerClass(TempratureTimeSeriesPartitioner.class);
		job.setGroupingComparatorClass(TempratureTimeSeriesGroupComparator.class);
		job.setReducerClass(TempratureTimeSeriesReducer.class);
		
		job.setOutputKeyClass(TextIntPair.class);
		job.setOutputValueClass(SumCountPair.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
