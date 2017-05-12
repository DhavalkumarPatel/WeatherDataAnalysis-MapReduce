package mr.hw2.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * This class is used to store minimum and maximum temperature sum with count
 * for specific station. It implements Writable interface as it is going to be
 * used as a value.
 * @author dspatel
 */
public class SumCountPair implements Writable
{
	private DoubleWritable maxTempSum, minTempSum;
    private IntWritable maxTempCount, minTempCount;
    
    /**
	 * Initialize the object with sum and count values
	 */
    public SumCountPair() 
    {
        this.maxTempSum = new DoubleWritable(0);
        this.maxTempCount = new IntWritable(0);
        
        this.minTempSum = new DoubleWritable(0);
        this.minTempCount = new IntWritable(0);
    }
    
    /**
     * set the min & max temperature sum and count of a station from the arguments passed
     * @param maxTempSum
     * @param maxTempCount
     * @param minTempSum
     * @param minTempCount
     */
    public void set(double maxTempSum, int maxTempCount, double minTempSum, int minTempCount)
    {
    	this.maxTempSum.set(maxTempSum);
    	this.maxTempCount.set(maxTempCount);
    	
    	this.minTempSum.set(minTempSum);
    	this.minTempCount.set(minTempCount);
    }
    
    /**
     * adds the min & max temperature sum & count passed in sumCountPair into current object if passed
     * object is not null
     * @param sumCountPair
     */
    public void mergeSumCount(SumCountPair sumCountPair)
    {
    	if(null != sumCountPair)
    	{
    		this.maxTempSum.set(this.maxTempSum.get() + sumCountPair.getMaxTempSum().get());
            this.maxTempCount.set(this.maxTempCount.get() + sumCountPair.getMaxTempCount().get());
            
            this.minTempSum.set(this.minTempSum.get() + sumCountPair.getMinTempSum().get());
            this.minTempCount.set(this.minTempCount.get() + sumCountPair.getMinTempCount().get());
    	}
    }

    /**
	 * @return the current max temperature sum of a station
	 */
	public DoubleWritable getMaxTempSum() 
	{
		return maxTempSum;
	}
	
	/**
	 * @return the current max temperature count of a station
	 */
	public IntWritable getMaxTempCount() 
	{
		return maxTempCount;
	}

    /**
	 * @return the current min temperature sum of a station
	 */
	public DoubleWritable getMinTempSum() 
	{
		return minTempSum;
	}

	/**
	 * @return the current min temperature count of a station
	 */
	public IntWritable getMinTempCount() 
	{
		return minTempCount;
	}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
	    maxTempSum.readFields(in);
	    maxTempCount.readFields(in);
	    
	    minTempSum.readFields(in);
	    minTempCount.readFields(in);
	}

    @Override
    public void write(DataOutput out) throws IOException 
    {
    	maxTempSum.write(out);
    	maxTempCount.write(out);
    	
    	minTempSum.write(out);
    	minTempCount.write(out);
    }
    
    /**
	 * @return the mean min & max temperature average of a station as a comma separated
	 * string where average for 0 count station is shown as NULL 
	 */
    @Override
    public String toString() 
    {
    	String meanMinTempStr = "NULL";
		if (minTempCount.get() > 0) 
		{
			meanMinTempStr = minTempSum.get() / minTempCount.get() + "";
		}
		
    	String meanMaxTempStr = "NULL";
		if (maxTempCount.get() > 0) 
		{
			meanMaxTempStr = maxTempSum.get() / maxTempCount.get() + "";
		}

		return meanMinTempStr + ", " + meanMaxTempStr;
		
    }
}
