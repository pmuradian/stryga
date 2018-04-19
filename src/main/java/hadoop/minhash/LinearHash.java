package hadoop.minhash;

/**
 * Created by Azazel on 4/4/18.
 */

/** f(x) = ax + b */
public class LinearHash  {
  private final int a;
  private final int b;

  private static final int SMALLER_TWIN_PRIME = 2147482949;
  private static final char[] hexDigits = "0123456789abcdef".toCharArray();

  public LinearHash(int a, int b)
  {
    this.a = a;
    this.b = b;
  }

  public static byte[] intToByteArray(int number) {
    return new byte[]{
        (byte)(number >>> 32),
        (byte)(number >>> 16),
        (byte)(number >>> 8 ),
        (byte) number
    };
  }

  public int hashAsInt(int input) {
    byte[] bytes = intToByteArray(input);

    return hashAsInt(bytes);
  }

  public int hashAsInt(byte[] bytes)
  {
    int hashValue = 31;

    for (int byteVal : bytes) {
      hashValue *= a * byteVal;
      hashValue += b;
    }

    return Math.abs(hashValue % SMALLER_TWIN_PRIME);
  }

  public String hashAsString(byte[] bytes) {
    StringBuilder sb = new StringBuilder(2 * bytes.length);

    for(int i = 0; i < bytes.length; ++i) {
      byte b = bytes[i];
      sb.append(hexDigits[b >> 4 & 15]).append(hexDigits[b & 15]);
    }

    return sb.toString();
  }

  public String hashAsString(int number) {
    return hashAsString(intToByteArray(number));
  }
}
