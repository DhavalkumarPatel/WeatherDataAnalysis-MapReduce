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

import mr.hw2.datatypes.SumCountPair;
import mr.hw2.utility.WeatherDataParser;

/**
 * NoCombiner - This program has a Mapper and a Reducer class with no custom setup
 * or cleanup methods, and no Combiner or custom Partitioner.
 * @author dspatel
 */
public class NoCombiner 
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

				if(parser.isMaxTemp()) 
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
		@Override
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
	 * main method to execute the Climate Analysis with No Combining MapReduce Job
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception 
	{
		if (args.length != 2) 
		{
			System.err.println("Usage: NoCombiner <input path> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		BasicConfigurator.configure();

		Job job = Job.getInstance(conf, "Climate Analysis with No Combining");
		job.setJarByClass(NoCombiner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MeanTemperatureMapper.class);
		job.setReducerClass(MeanTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SumCountPair.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
