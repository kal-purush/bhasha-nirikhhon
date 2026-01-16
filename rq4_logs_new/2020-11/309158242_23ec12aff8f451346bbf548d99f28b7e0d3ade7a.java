package ru.otus.algo.bits;

import java.math.BigInteger;

public class Bits {

    public static void main(String[] args) {
        new Bits().queen(54);
    }

    public BigInteger[] king(int pos) {
        long binaryIndex = 1L << pos;
        long noA = 0xfefefefefefefefeL;
        long noH = 0x7f7f7f7f7f7f7f7fL;
        long mask = ((binaryIndex & noA) << 7) | (binaryIndex << 8) | ((binaryIndex & noH) << 9) |
                    ((binaryIndex & noA) >> 1)                      | ((binaryIndex & noH) << 1) |
                    ((binaryIndex & noA) >> 9) | (binaryIndex >> 8) | ((binaryIndex & noH) >> 7);
        if (pos == 63) { //cтранное поведение для 63, на уроке не воспроизводилось
            mask &= 0x40ffffffffffffffL;
        }
        return getResult(mask);
    }

    public BigInteger[] horse(int pos) {
        long binaryIndex = 1L << pos;
        long noA = 0xfefefefefefefefeL;
        long noB = 0xfdfdfdfdfdfdfdfdL;
        long noG = 0xbfbfbfbfbfbfbfbfL;
        long noH = 0x7f7f7f7f7f7f7f7fL;
        long mask = ((binaryIndex & noA) << 15) | ((binaryIndex & noH) << 17) |
                    ((binaryIndex & noA & noB) << 6)  | ((binaryIndex & noH & noG) << 10) |
                    ((binaryIndex & noA & noB) >> 10) | ((binaryIndex & noH & noG) >> 6)  |
                    ((binaryIndex & noA) >> 17 ) | ((binaryIndex & noH) >> 15);

        if (pos == 63) { //снова cтранное поведение для 63
            mask &= 0x20400000000000L;
        }

        return getResult(mask);
    }

    public BigInteger[] bishop(int pos) {
        long mask = getBishopMask(pos);
        return getResult(mask);
    }

    public BigInteger[] rook(int pos) {
        long mask = getRookMask(pos);
        return getResult(mask);
    }

    public BigInteger[] queen(int pos) {
        long mask = getRookMask(pos) | getBishopMask(pos);
        return getResult(mask);
    }

    private long getBishopMask(int pos) {
        long binaryIndex = 1L << pos;

        long mask = 0;
        int counter = pos;
        int step = 7;
        while (counter < 56 && counter % 8 != 0) {
            mask |= ((binaryIndex) << step);
            counter+=7;
            step+=7;
        }
        counter = pos;
        step = 9;
        while (counter < 56 && (counter + 1) % 8 != 0) {
            mask |= ((binaryIndex) << step);
            counter+=9;
            step+=9;
        }
        counter = pos;
        step = 9;
        while (counter > 7 && counter % 8 != 0) {
            mask |= ((binaryIndex) >> step);
            counter-=9;
            step+=9;
        }
        counter = pos;
        step = 7;
        while (counter > 7 && (counter + 1) % 8 != 0) {
            mask |= ((binaryIndex) >> step);
            counter-=7;
            step+=7;
        }

        if (pos == 63) { //снова cтранное поведение для 63
            mask &= 0x40201008040201L;
        }
        return mask;
    }

    private long getRookMask(int pos) {
        long binaryIndex = 1L << pos;

        long mask = 0;
        int counter = pos;
        int step = 8;
        while (counter < 56) {
            mask |= ((binaryIndex) << step);
            counter+=8;
            step+=8;
        }
        counter = pos;
        step = 1;
        while ((counter + 1) % 8 != 0) {
            mask |= ((binaryIndex) << step);
            counter+=1;
            step+=1;
        }
        counter = pos;
        step = 1;
        while (counter % 8 != 0) {
            mask |= ((binaryIndex) >> step);
            counter-=1;
            step+=1;
        }
        counter = pos;
        step = 8;
        while (counter > 7) {
            mask |= ((binaryIndex) >> step);
            counter-=8;
            step+=8;
        }
        if (pos == 63) { //снова cтранное поведение для 63
            mask &= 0x7f80808080808080L;
        }
        return mask;
    }

    private BigInteger[] getResult(long mask) {
        BigInteger[] result = new BigInteger[2];
        result[0] = BigInteger.valueOf(Long.bitCount(mask));
        result[1] = new BigInteger(Long.toUnsignedString(mask));
        System.out.println(result[1]);
        return result;
    }

}