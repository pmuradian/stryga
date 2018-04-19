package hadoop.lsh;

import java.io.IOException;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Azazel on 4/10/18.
 */
public class LSHSimilarityReducer extends Reducer<Text, Text, Text, FloatWritable> {
  private float bands;
  private float threshold;
  private Multiset<String> counters;

  @Override
  public void reduce(Text id, Iterable<Text> values, Context ctx) throws IOException, InterruptedException
  {
    counters = HashMultiset.create();
    String a = id.toString().trim();

    for (Text x : values) {
      counters.add(x.toString());
    }

    for (Multiset.Entry<String> entry : counters.entrySet()) {
      float fraction = entry.getCount() / bands;

      if (fraction > threshold) {
        String b = entry.getElement().trim();
        ctx.write(new Text(String.format("%s - %s", a, b)), new FloatWritable(fraction));
      }
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException
  {
    int rows = context.getConfiguration().getInt("lsh-band-size", 5);
    this.bands = context.getConfiguration().getInt("lsh-bands", 20);
    this.threshold = (float) Math.pow(1 / bands, 1 / (float) rows);
  }
}