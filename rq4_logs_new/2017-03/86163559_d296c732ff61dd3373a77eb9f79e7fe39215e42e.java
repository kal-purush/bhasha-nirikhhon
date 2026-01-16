
package cx.ath.matthew.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for {@link Hexdump}.
 *
 * @author Markus Rathgeb - Initial contribution
 *
 */
public class HexdumpTest {

    /**
     * Test the {@link Hexdump#toHex(byte[])} function.
     */
    @Test
    public void testToHexComplete() {
        final byte[] buf = new byte[] { (byte) 0x00, (byte) 0xAF, (byte) 0xFE, (byte) 0xDE, (byte) 0xAD };
        final String str = Hexdump.toHex(buf);
        Assert.assertEquals("00 af fe de ad ", str);
    }

    /**
     * Test the {@link Hexdump#toHex(byte[], int, int)} function.
     */
    @Test
    public void testToHexPart() {
        final byte[] buf = new byte[] { (byte) 0x00, (byte) 0xAF, (byte) 0xFE, (byte) 0xDE, (byte) 0xAD };
        final String str = Hexdump.toHex(buf, 2, 2);
        Assert.assertEquals("fe de ", str);
    }

    /**
     * Test the {@link Hexdump#toAscii(byte[])} function using non-readable characters.
     */
    @Test
    public void testToAsciiCompleteNonReadable() {
        final byte[] buf = new byte[] { (byte) 0x00, (byte) 0xAF, (byte) 0xFE, (byte) 0xDE, (byte) 0xAD };
        final String str = Hexdump.toAscii(buf);
        Assert.assertEquals(".....", str);
    }

    /**
     * Test the {@link Hexdump#toAscii(byte[])} function using readable characters.
     */
    @Test
    public void testToAsciiCompleteReadable() {
        final byte[] buf = new byte[] { (byte) '!', (byte) 'a', (byte) 'f', (byte) 'f', (byte) 'e', (byte) '-' };
        final String str = Hexdump.toAscii(buf);
        Assert.assertEquals("!affe-", str);
    }

    // @Test
    // public void testToAsciiByteArrayIntInt() {
    // fail("Not yet implemented");
    // }

    /**
     * Test the {@link Hexdump#format(byte[])} function.
     */
    @Test
    public void testFormatComplete() {
        // Check that there is no exception caught.
        for (int sz = 256; sz >= 0; --sz) {
            final byte[] buf = new byte[sz];
            for (int i = buf.length - 1; i >= 0; --i) {
                buf[i] = (byte) i;
            }

            Hexdump.format(buf);
        }

        // Assert.assertEquals(".....", str);
    }

    /**
     * Test the {@link Hexdump#toByteArray(byte[])} function.
     */
    @Test
    public void testToByteArrayComplete() {
        final byte[] buf = new byte[] { (byte) 0x00, (byte) 0xAF, (byte) 0xFE, (byte) 0xDE, (byte) 0xAD };
        final String str = Hexdump.toByteArray(buf);
        Assert.assertEquals("0x00,0xaf,0xfe,0xde,0xad", str);
    }

    /**
     * Test the {@link Hexdump#toByteArray(byte[], int, int)} function.
     */
    @Test
    public void testToByteArrayPart() {
        final byte[] buf = new byte[] { (byte) 0x00, (byte) 0xAF, (byte) 0xFE, (byte) 0xDE, (byte) 0xAD };
        final String str = Hexdump.toByteArray(buf, 2, 4);
        Assert.assertEquals("0xfe,0xde", str);
    }

}