package com.example.justpoteito.security;

import android.content.Context;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RsaFileReader {

    public static byte[] readRsaFile(String fileName, Context context) {
        InputStream in = null;
        byte[] key = null;
        try {
            in = context.getResources().getAssets().open(fileName);
            key = readAllBytes(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            return key;
        }
    }

    private static byte[] readAllBytes(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        copyAllBytes(in, out);
        return out.toByteArray();
    }

    private static int copyAllBytes(InputStream in, OutputStream out)
            throws IOException {
        int byteCount = 0;
        byte[] buffer = new byte[4096];
        while (true) {
            int read = in.read(buffer);
            if (read == -1) {
                break;
            }
            out.write(buffer, 0, read);
            byteCount += read;
        }
        return byteCount;
    }
}