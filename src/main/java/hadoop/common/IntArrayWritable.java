package hadoop.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Created by Azazel on 4/2/18.
 */
public class IntArrayWritable extends ArrayWritable {
  public IntArrayWritable()
  {
    super(IntWritable.class);
  }

  @Override
  public IntWritable[] get()
  {
    return (IntWritable[]) super.get();
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    for (IntWritable el : get()) {
      el.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    String line = in.readLine();
    String[] values = line.split(" ");
    IntWritable[] vals = new IntWritable[values.length];

    int i = 0;
    for (String s : values) {
      vals[i] = new IntWritable(Integer.parseInt(s));
      i++;
    }
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder();

    builder.append("[");
    for (IntWritable el : get()) {
      builder.append(el.get()).append(",");
    }

    builder.setCharAt(builder.length() -1 , ']');

    return builder.toString();
  }
}
