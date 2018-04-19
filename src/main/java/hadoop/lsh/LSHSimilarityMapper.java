package hadoop.lsh;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Azazel on 4/10/18.
 */
public class LSHSimilarityMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text values, Context ctx) throws IOException,
      InterruptedException {
    List<Text> data = Lists.newArrayList();
    System.out.println("Values : " + values);
    String[] text = values.toString().trim().replace("]", "").replace("[", "").split(",");

    for (String s : text) {
      data.add(new Text(s));
    }
    Collections.sort(data);
    for (Iterator<Text> it = data.iterator(); it.hasNext(); ) {
      Text cur = it.next();
      it.remove();
      for (Text doc : data) {
        ctx.write(cur, doc);
      }
    }
  }
}
