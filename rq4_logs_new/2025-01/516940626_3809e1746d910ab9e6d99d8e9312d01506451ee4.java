package com.numo.api.comm.audio;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AudioMergeTest {
    String filePath = "/Users/hyun/Projects/wordApp/tmp";

    String silenceFilePath = "/Users/hyun/Projects/wordApp/tmp/silence/silence.mp3";

    @Test
    void mergeSound() throws IOException {
        File file = new File(filePath);
        File[] files = file.listFiles();

        File silence = new File(silenceFilePath);
        byte[] silenceByte = new byte[(int)silence.length()];

        FileInputStream silenceFi = new FileInputStream(silence);
        silenceFi.read(silenceByte);

        List<byte[]> byteArrayList = new ArrayList<>();

        FileOutputStream fo = new FileOutputStream(filePath + "/merge/merge.mp3");

        for (File f : files) {
            if (f.isFile()) {
                int size = (int) f.length();
                byte[] readByte = new byte[size];
                FileInputStream fi = new FileInputStream(f);
                int n = fi.read(readByte);
                if (readByte.length > 0) {
                    byteArrayList.add(readByte);
                    byteArrayList.add(createSilence(silenceByte, 2));
                }
            }
        }

        byte[] result = mergeByteArrays(byteArrayList);
        fo.write(result);
    }

    @Test
    void mergeSound1() throws IOException {
        File file = new File(filePath);
        List<String> searchFileNames = List.of("setting.mp3", "한국.mp3", "weight.mp3");

        File[] files = file.listFiles(((dir, name) -> {
            List<String> list = searchFileNames.stream().filter(n -> Objects.equals(n, name)).toList();
            return !list.isEmpty();
        }));

        File silence = new File(silenceFilePath);
        byte[] silenceByte = new byte[(int)silence.length()];

        FileInputStream silenceFi = new FileInputStream(silence);
        silenceFi.read(silenceByte);

        List<byte[]> byteArrayList = new ArrayList<>();

        FileOutputStream fo = new FileOutputStream(filePath + "/merge/merge.mp3");

        for (File f : files) {
            System.out.println(f.getName());
            if (f.isFile()) {
                int size = (int) f.length();
                byte[] readByte = new byte[size];
                FileInputStream fi = new FileInputStream(f);
                int n = fi.read(readByte);
                if (readByte.length > 0) {
                    byteArrayList.add(readByte);
                    byteArrayList.add(createSilence(silenceByte, 2));
                }
            }
        }

        byte[] result = mergeByteArrays(byteArrayList);
        fo.write(result);
    }

    public static byte[] mergeByteArrays(List<byte[]> byteArrayList) {
        int totalLength = byteArrayList.stream().mapToInt(byteArray -> byteArray.length).sum();

        byte[] mergedArray = new byte[totalLength];
        int currentPosition = 0;

        for (byte[] byteArray : byteArrayList) {
            System.arraycopy(byteArray, 0, mergedArray, currentPosition, byteArray.length);
            currentPosition += byteArray.length;
        }

        return mergedArray;
    }

    private static byte[] createSilence(byte[] silence, int gapInSeconds) {
        int size = silence.length * gapInSeconds;
        byte[] newSilence = new byte[size];

        int currentPosition = 0;

        for (int i=0; i<gapInSeconds; i++) {
            System.arraycopy(silence, 0, newSilence, currentPosition, silence.length);
            currentPosition += silence.length;
        }

        return newSilence;
    }

}