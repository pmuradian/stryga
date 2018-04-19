package hadoop.minhash;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import hadoop.common.TextArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Azazel on 4/4/18.
 */
public class MinhashReducer extends Reducer<LongWritable, Text, LongWritable, TextArrayWritable> {
  @Override
  protected void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException
  {
    List<Text> documents = Lists.newArrayList();

    for (Text x : values) {
      documents.add(new Text(x));
    }

    if (documents.size() > 1) {
      TextArrayWritable out = new TextArrayWritable();
      out.set(documents.toArray(new Text[documents.size()]));
      context.write(key, out);
    }
  }
}
