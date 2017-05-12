package mr.hw2.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * This class is used to store stationId (as Text) and Year (as IntWritable).
 * It implements WritableComparable interface as it is going to be
 * used as a key.
 * @author dspatel
 */
public class TextIntPair implements WritableComparable<TextIntPair>
{
	private Text text;
	private IntWritable integer;

	public TextIntPair() 
    {
        this.text = new Text();
        this.integer = new IntWritable();
    }
	
	public void set(String text, int integer) 
	{
		this.text = new Text(text);
		this.integer = new IntWritable(integer);
	}
	
	public Text getText() 
	{
		return text;
	}
	
	public IntWritable getInteger() 
	{
		return integer;
	}
	
	@Override
	public void write(DataOutput out) throws IOException 
	{
		text.write(out);
    	integer.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException 
	{
	    text.readFields(in);
	    integer.readFields(in);
	}

	/**
	 * while comparing two keys, we first compare the Text and if the two
	 * Texts are same then only compare the IntWritable. Due to this method
	 * our data at reducer will be sorted by stationId, Year order.
	 * This method replaces the need of KeyComparator. 
	 */
	@Override
	public int compareTo(TextIntPair argPair) 
	{
		int cmp = this.text.compareTo(argPair.getText());
	    if (cmp != 0) 
	    {
	        return cmp;
	    }
	    return this.integer.compareTo(argPair.getInteger());
	}

}
