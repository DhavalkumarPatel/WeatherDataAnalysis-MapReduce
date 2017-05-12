package mr.hw2.part1;

import java.io.IOException;

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
import org.apache.log4j.Logger;

import mr.hw2.datatypes.SumCountPair;
import mr.hw2.utility.WeatherDataParser;

/**
 * Combiner - This program uses a Combiner along with a Mapper and a Reducer class
 * @author dspatel
 */
public class Combiner
{
	/**
	 * Mapper class with only map function
	 * @author dspatel
	 */
	public static class MeanTemperatureMapper extends Mapper<Object, Text, Text, SumCountPair> 
	{
		private WeatherDataParser parser = new WeatherDataParser();
		private Text stationId = new Text();
		private SumCountPair sumCountPair = new SumCountPair();

		/**
		 * map function parses each input record and for each valid record, it emits the stationId as a Text
		 * and min/max temperature with count as a SumCountPair object
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			if(parser.parse(value.toString()))
			{
				stationId.set(parser.getStationId());

				if (parser.isMaxTemp()) 
				{
					sumCountPair.set(parser.getTemperature(), 1, 0, 0);
				} 
				else 
				{
					sumCountPair.set(0, 0, parser.getTemperature(), 1);
				}
				
				context.write(stationId, sumCountPair);
			}
		}
	}

	/**
	 * Combiner class with setup, reduce and cleanup function
	 * setup and cleanup function are only used to calculate the total number of input records to
	 * specific combiner call and total number output records from that call
	 * @author dspatel
	 */
	public static class MeanTemperatureCombiner extends Reducer<Text, SumCountPair, Text, SumCountPair> 
	{
		private Logger logger = Logger.getLogger(MeanTemperatureCombiner.class);
		private int inputCount, outputCount;
		
		/**
		 * this method initializes the count values to 0 before each new combiner call within one Map task
		 */
		@Override
		protected void setup(Reducer<Text, SumCountPair, Text, SumCountPair>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			inputCount = 0;
			outputCount = 0;
		}
		
		/**
		 * reduce function gets the stationId as a key and iterable list of SumCountPairs as values
		 * for each station reduce function combines/merges the sumCountPairs and emits the stationId
		 * as a key and combined sumCountPair object as a value
		 * it also counts the input and output record counts
		 */
		@Override
		public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException 
		{
			SumCountPair sumCountPair = new SumCountPair();
			for (SumCountPair val : values) 
			{
				sumCountPair.mergeSumCount(val);
				inputCount++;
			}

			outputCount++;
			context.write(key, sumCountPair);
		}
		
		/**
		 * This method logs the input and output records count processed in the specific combiner call
		 */
		@Override
		protected void cleanup(Reducer<Text, SumCountPair, Text, SumCountPair>.Context context) throws IOException, InterruptedException 
		{
			super.cleanup(context);
			logger.info("Combiner executed for task attempt ID : " + context.getTaskAttemptID() +
					" :: [input records -> output records] = [" + inputCount + " -> " + outputCount + "]");
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
	 * main method to execute the Climate Analysis with Combiner MapReduce Job
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		if (args.length != 2) 
		{
			System.err.println("Usage: Combiner <input path> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		BasicConfigurator.configure();

		Job job = Job.getInstance(conf, "Climate Analysis with Combiner");
		job.setJarByClass(Combiner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MeanTemperatureMapper.class);
		job.setCombinerClass(MeanTemperatureCombiner.class);
		job.setReducerClass(MeanTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SumCountPair.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
