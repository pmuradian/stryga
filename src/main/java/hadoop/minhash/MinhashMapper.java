package hadoop.minhash;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Azazel on 4/3/18.
 */
public class MinhashMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  private int[] hashValues;
  private int minhashLength;
  private LinearHash[] hashes;
  private LinearHash bandHasher;
  private int bands;
  private int rows;

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    for (int i = 0; i < minhashLength; i++) {
      hashValues[i] = Integer.MAX_VALUE;
    }

    int start = value.toString().indexOf('[') + 1;
    int end = value.toString().indexOf(']');
    String joinedValues = value.toString().substring(start, end);

    String[] values = joinedValues.toString().split(",");
    System.out.println("Joined values: " + joinedValues);
    System.out.println("Values: " + values.length);

    for (int i = 0; i < minhashLength; i++) {
      LinearHash hasherAtHand = hashes[i];
      for (String val : values) {
        int hashAtHand = hasherAtHand.hashAsInt(Integer.parseInt(val));

        if(hashAtHand < hashValues[i]) {
          hashValues[i] = hashAtHand;
        }
      }
    }

    byte[] bandHashBuf = new byte[4 * rows];
    int band = 0;
    int from  = 0;
    for (int i = 0;  i < minhashLength; i++) {
      System.arraycopy(LinearHash.intToByteArray(hashValues[i]), 0, bandHashBuf, from, 4);
      from += 4;

      if ((i + 1) % rows == 0 && i != 0) {
        String bandHash = bandHasher.hashAsString(bandHashBuf);
        context.write(new LongWritable(key.get()), new Text(band + ":" + bandHash));
        bandHashBuf = new byte[4 * rows];
        from = 0;
        band++;
      }
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException
  {
    minhashLength = context.getConfiguration().getInt("minhash-length", 100);
    bands = context.getConfiguration().getInt("lsh-bands", 5);
    rows = context.getConfiguration().getInt("lsh-band-size", 20);
    hashValues = new int[minhashLength];
    hashes = new LinearHash[minhashLength];

    Random r = new Random(100);

    for (int i = 0; i < minhashLength; i++) {
      hashes[i] = new LinearHash(r.nextInt(), r.nextInt());
    }

    bandHasher = new LinearHash(r.nextInt(), r.nextInt());
  }
}
