package com.joizhang.naiverpc.netty.serialize.gson;

import com.google.gson.Gson;
import com.joizhang.naiverpc.serialize.ObjectInput;

import java.io.*;
import java.lang.reflect.Type;

public class GsonObjectInput implements ObjectInput {

    private final BufferedReader reader;

    private final Gson gson;

    public GsonObjectInput(InputStream inputStream) {
        this(new InputStreamReader(inputStream));
    }

    public GsonObjectInput(Reader reader) {
        this.reader = new BufferedReader(reader);
        this.gson = new Gson();
    }

    @Override
    public boolean readBool() throws IOException {
        return read(boolean.class);
    }

    @Override
    public byte readByte() throws IOException {
        return read(byte.class);
    }

    @Override
    public short readShort() throws IOException {
        return read(short.class);
    }

    @Override
    public int readInt() throws IOException {
        return read(int.class);
    }

    @Override
    public long readLong() throws IOException {
        return read(long.class);
    }

    @Override
    public float readFloat() throws IOException {
        return read(float.class);
    }

    @Override
    public double readDouble() throws IOException {
        return read(double.class);
    }

    @Override
    public String readUTF() throws IOException {
        return read(String.class);
    }

    @Override
    public byte[] readBytes() throws IOException {
        return readLine().getBytes();
    }

    @Override
    public Object readObject() throws IOException, ClassNotFoundException {
        String json = readLine();
        return gson.fromJson(json, Object.class);
    }

    @Override
    public <T> T readObject(Class<T> cls) throws IOException, ClassNotFoundException {
        return read(cls);
    }

    @Override
    public <T> T readObject(Class<T> cls, Type type) throws IOException, ClassNotFoundException {
        return read(cls);
    }

    private <T> T read(Class<T> cls) throws IOException {
        String json = readLine();
        return gson.fromJson(json, cls);
    }

    private String readLine() throws IOException {
        String line = reader.readLine();
        if (line == null || line.trim().length() == 0) {
            throw new EOFException();
        }
        return line;
    }

}