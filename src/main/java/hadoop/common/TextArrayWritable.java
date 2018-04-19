package hadoop.common;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by Azazel on 4/2/18.
 */
public class TextArrayWritable extends ArrayWritable {
  public TextArrayWritable()
  {
    super(Text.class);
  }

  public String toString() {
    return Arrays.asList(toStrings()).toString();
  }
}
