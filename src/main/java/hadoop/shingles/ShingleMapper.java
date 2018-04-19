package hadoop.shingles;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Azazel on 4/2/18.
 */
public class ShingleMapper extends Mapper<Object, Text, LongWritable, IntWritable> {
  private int shingleSize;

  private Set<Integer> createShingles(String doc)
  {
    Set<Integer> shingles = new HashSet<Integer>();

    for (int i = 0; i < doc.length() - shingleSize - 1; i++) {
      shingles.add(doc.substring(i, i + shingleSize).hashCode());
    }

    return shingles;
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException
  {
    shingleSize = context.getConfiguration().getInt("shingle-length", 5);
  }

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
  {
    String strValue = value.toString();
    String strId = strValue.substring(0, strValue.indexOf(","));

    try {
      Integer docId = Integer.parseInt(strId);
      String clean = strValue.substring(strValue.indexOf(",") + 1, strValue.length()).replaceAll("[^a-zA-Z -]", "");
      System.out.println("Id:" + docId + " with value " + clean);
      Set<Integer> shingles = createShingles(clean);

      System.out.println(shingles.size());

      for (Integer shingle : shingles) {
        context.write(new LongWritable(docId), new IntWritable(shingle));
      }
    } catch (NumberFormatException e) {
      System.out.println("Got \"" + strId + "\" as ID for a document. Skipping ...");
    }
  }
}