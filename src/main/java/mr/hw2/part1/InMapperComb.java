package mr.hw2.part1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import mr.hw2.datatypes.SumCountPair;
import mr.hw2.utility.WeatherDataParser;

/**
 * InMapperComb – This program uses in-mapper combining to reduce data traffic from 
 * Mappers to Reducers and it does not use Combiner.
 * @author dspatel
 */
public class InMapperComb 
{
	/**
	 * Mapper class with setup, map and cleanup function
	 * @author dspatel
	 */
	public static class MeanTemperatureMapper extends Mapper<Object, Text, Text, SumCountPair> 
	{
		WeatherDataParser parser = new WeatherDataParser();
		
		// Map for storing sumCountPair (min & max temperature sums with count) object per stationId
		private Map<Text, SumCountPair> stationRecordsMap;

		/**
		 * setup method initializes the stationRecordsMap
		 */
		@Override
		protected void setup(Mapper<Object, Text, Text, SumCountPair>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			stationRecordsMap = new HashMap<Text, SumCountPair>();
		}
		
		/**
		 * map function parses each input record and for each valid record, it creates a sumCountPair object
		 * for this stationId. Then it adds this sumCountPair object into stationRecordsMap for this stationId
		 * if this stationId is not present in map else it merges this sumCountPair object with existing one.
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			if(parser.parse(value.toString()))
			{
				Text stationId = new Text(parser.getStationId());
				SumCountPair sumCountPair = new SumCountPair();

				if (parser.isMaxTemp()) 
				{
					sumCountPair.set(parser.getTemperature(), 1, 0, 0);
				} 
				else 
				{
					sumCountPair.set(0, 0, parser.getTemperature(), 1);
				}
				
				sumCountPair.mergeSumCount(stationRecordsMap.get(stationId));
				stationRecordsMap.put(stationId, sumCountPair);
			}
		}
		
		/**
		 * cleanup method iterates through each stationId in stationRecordsMap and emits the stationId
		 * as a key and merged sumCountPair object as a value
		 */
		@Override
		protected void cleanup(Mapper<Object, Text, Text, SumCountPair>.Context context) throws IOException, InterruptedException 
		{
			super.cleanup(context);
			Iterator<Map.Entry<Text, SumCountPair>> iterator = stationRecordsMap.entrySet().iterator();
		    while (iterator.hasNext()) 
		    {
		        Map.Entry<Text, SumCountPair> pair = iterator.next();
		        context.write(pair.getKey(), pair.getValue());
		    }
		}
	}

	/**
	 * Reducer class with only reduce function
	 * @author dspatel
	 */
	public static class MeanTemperatureReducer extends Reducer<Text, SumCountPair, Text, NullWritable> 
	{
		private Text result = new Text();

		/**
		 * reduce function gets the stationId as a key and iterable list of SumCountPairs as values
		 * for each station reduce function calculates mean minimum and mean maximum temperatures
		 * and emits the result as Text 
		 */
		public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException 
		{
			SumCountPair sumCountPair = new SumCountPair();
			
			for (SumCountPair val : values) 
			{
				sumCountPair.mergeSumCount(val);
			}

			result.set(key + ", " + sumCountPair.toString());
			context.write(result, NullWritable.get());
		}
	}

	/**
	 * main method to execute the Climate Analysis with In Mapper Combining MapReduce Job
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		if (args.length != 2) 
		{
			System.err.println("Usage: InMapperComb <input path> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		BasicConfigurator.configure();

		Job job = Job.getInstance(conf, "Climate Analysis with In Mapper Combining");
		job.setJarByClass(InMapperComb.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MeanTemperatureMapper.class);
		job.setReducerClass(MeanTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SumCountPair.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
