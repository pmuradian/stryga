package hadoop.shingles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import hadoop.common.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Azazel on 4/2/18.
 */
public class ShingleReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntArrayWritable> {
  @Override
  protected void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException
  {
    List<IntWritable> shingles = new ArrayList<IntWritable>();

    for (IntWritable v : values) {
      IntWritable shingle = new IntWritable();
      shingle.set(v.get());
      shingles.add(shingle);
    }

    IntArrayWritable out = new IntArrayWritable();
    out.set(shingles.toArray(new IntWritable[shingles.size()]));
    context.write(key, out);
  }
}
