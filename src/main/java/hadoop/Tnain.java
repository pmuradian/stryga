package hadoop;

import java.io.IOException;

import hadoop.common.IntArrayWritable;
import hadoop.common.TextArrayWritable;
import hadoop.lsh.LSHSimilarityMapper;
import hadoop.lsh.LSHSimilarityReducer;
import hadoop.minhash.MinhashMapper;
import hadoop.minhash.MinhashReducer;
import hadoop.shingles.ShingleMapper;
import hadoop.shingles.ShingleReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Tnain extends Configured implements Tool {
  private final static String SHINGLES = "shingles";
  private final static String MINHASHES = "minhashes";

  public int run(String[] args) throws Exception
  {
    Configuration conf = getConf();

    conf.setInt("shingle-length", 5);
    conf.setInt("minhash-length", 100);
    conf.setInt("lsh-bands", 20);
    conf.setInt("lsh-band-size", 5);

    Path input = new Path(args[0]);
    Path out = new Path(args[1]);
    out.getFileSystem(conf).delete(out, true);
    return ((doShingle(input, new Path(SHINGLES), conf) &&
        doMinhash(new Path(SHINGLES), new Path(MINHASHES), conf) &&
        doLSH(new Path(MINHASHES), out, conf)) ? 0 : 1);
  }

  private static boolean doShingle(Path input, Path out, Configuration conf)
      throws IOException, ClassNotFoundException, InterruptedException
  {
    Job job = Job.getInstance(conf, "tnain-shingle");
    job.setJarByClass(Tnain.class);

    job.setMapperClass(ShingleMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(ShingleReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(IntArrayWritable.class);

    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, out);

    return job.waitForCompletion(true);
  }

  private static boolean doMinhash(Path input, Path out, Configuration conf)
      throws IOException, ClassNotFoundException, InterruptedException
  {
    Job job = Job.getInstance(conf, "tnain-minhash");
    job.setJarByClass(Tnain.class);

    job.setMapperClass(MinhashMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(MinhashReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(TextArrayWritable.class);

    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, out);

    return job.waitForCompletion(true);
  }

  private static boolean doLSH(Path input, Path out, Configuration conf)
      throws IOException, ClassNotFoundException, InterruptedException
  {
    Job job = Job.getInstance(conf, "tnain-lsh");
    job.setJarByClass(Tnain.class);

    job.setMapperClass(LSHSimilarityMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(LSHSimilarityReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, out);

    return job.waitForCompletion(true);
  }

  public static void main(String[] args)
  {
    try {
      int res = ToolRunner.run(new Configuration(), new Tnain(), args);
      System.exit(res);
    } catch (Exception e) {
      System.out.println("Unable to run tnain");
      e.printStackTrace();
    }
  }
}
