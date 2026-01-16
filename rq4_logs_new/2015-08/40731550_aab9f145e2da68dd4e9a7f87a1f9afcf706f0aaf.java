package wh.lanmsg.io;

import java.util.Arrays;

/**
 * Created on Aug 16, 2015.
 * 
 * @author Wes Hampson
 */
public class MessageDataOutputStream
{
    private static final int DEFAULT_INITIAL_CAPACITY = 32;
    
    private byte[] buf;
    private int offset;
    private int mark;
    
    public MessageDataOutputStream()
    {
        this(DEFAULT_INITIAL_CAPACITY);
    }
    
    public MessageDataOutputStream(int initialCapacity)
    {
        buf = new byte[initialCapacity];
        offset = 0;
        mark = 0;
    }
    
    public void mark()
    {
        mark = offset;
    }
    
    public void reset()
    {
        seek(mark);
    }
    
    public void resetMark()
    {
        mark = 0;
    }
    
    public int seek(int pos)
    {
        int diff;
        if (pos < 0) {
            throw new IndexOutOfBoundsException(String.format("%d", pos));
        }
        ensureCapacity(pos);
        diff = pos - offset;
        offset = pos;
        return diff;
    }
    
    public int skip(int n)
    {
        if (offset + n < 0) {
            throw new IndexOutOfBoundsException(
                    String.format("%d", offset + n));
        }
        ensureCapacity(offset + n);
        offset += n;
        return offset;
    }
    
    public void write(byte b)
    {
        ensureCapacity(offset + 1);
        buf[offset++] = b;
    }
    
    public void write(byte[] b)
    {
        for (int i = 0; i < b.length; i++) {
            write(b[i]);
        }
    }
    
    public void writeBoolean(boolean bool)
    {
        write(bool ? (byte) 1 : 0);
    }
    
    public void writeChar(char c)
    {
        write((byte) (c >>> 0));
        write((byte) (c >>> 8));
    }
    
    public void writeChar(char[] c)
    {
        for (int i = 0; i < c.length; i++) {
            writeChar(c[i]);
        }
    }
        
    public void writeFloat(float f)
    {
        writeInt(Float.floatToIntBits(f));
    }
    
    public void writeInt(int i)
    {
        write((byte) (i >>> 0));
        write((byte) (i >>> 8));
        write((byte) (i >>> 16));
        write((byte) (i >>> 24));
    }
    
    public void writeLong(long l)
    {
        writeInt((int) (l >>> 0));
        writeInt((int) (l >>> 32));
    }
    
    public void writeShort(short s)
    {
        write((byte) (s >>> 0));
        write((byte) (s >>> 8));
    }
    
    public byte[] toArray()
    {
        byte[] bufCopy = new byte[buf.length];
        System.arraycopy(buf, 0, bufCopy, 0, buf.length);
        return bufCopy;
    }
    
    private void ensureCapacity(int size)
    {
        if (buf.length < size) {
            expand(size - buf.length);
        }
    }
    
    private void expand(int len)
    {
        buf = Arrays.copyOf(buf, buf.length + len);
    }
}