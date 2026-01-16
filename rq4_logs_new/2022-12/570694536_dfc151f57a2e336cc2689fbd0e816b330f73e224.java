package com.barteam.barject.generator;

public class BarcodeBytecoder {

    private String getTemplate(char num) {
        return switch (num) {
            case '0' -> "LLLLLL";
            case '1' -> "LLGLGG";
            case '2' -> "LLGGLG";
            case '3' -> "LLGGGL";
            case '4' -> "LGLLGG";
            case '5' -> "LGGLLG";
            case '6' -> "LGGGLL";
            case '7' -> "LGLGLG";
            case '8' -> "LGLGGL";
            case '9' -> "LGGLGL";
            default -> null;
        };
    }
    private String getRCode(char num) {
        return switch (num) {
            case '0' -> "1110010";
            case '1' -> "1100110";
            case '2' -> "1101100";
            case '3' -> "1000010";
            case '4' -> "1011100";
            case '5' -> "1001110";
            case '6' -> "1010000";
            case '7' -> "1000100";
            case '8' -> "1001000";
            case '9' -> "1110100";
            default -> null;
        };
    }
    private String getLCode(char num) {
        return switch (num) {
            case '0' -> "0001101";
            case '1' -> "0011001";
            case '2' -> "0010011";
            case '3' -> "0111101";
            case '4' -> "0100011";
            case '5' -> "0110001";
            case '6' -> "0101111";
            case '7' -> "0111011";
            case '8' -> "0110111";
            case '9' -> "0001011";
            default -> null;
        };
    }
    private String getGCode(char num) {
        return switch (num) {
            case '0' -> "0100111";
            case '1' -> "0110011";
            case '2' -> "0011011";
            case '3' -> "0100001";
            case '4' -> "0011101";
            case '5' -> "0111001";
            case '6' -> "0000101";
            case '7' -> "0010001";
            case '8' -> "0001001";
            case '9' -> "0010111";
            default -> null;
        };
    }

    private String getCenterGuardCode() {
        return "01010";
    }
    private String getGuard(){
        return "101";
    }
    private String getLeftSideBytecode(String barcode) {
        StringBuilder stringBuilder = new StringBuilder();

        char firstNum = barcode.charAt(0);
        String leftSide = barcode.substring(1, 7);
        String template = getTemplate(firstNum);

        for (int i = 0; i < 6; i++) {
            if (template.charAt(i) == 'L') {
                stringBuilder.append(getLCode(leftSide.charAt(i)));
            } else {
                stringBuilder.append(getGCode(leftSide.charAt(i)));
            }
        }

        return stringBuilder.toString();
    }
    private String getRightSideBytecode(String barcode) {
        StringBuilder stringBuilder = new StringBuilder();

        String rightSide = barcode.substring(7, 12) + getControlNum(barcode);

        for (int i = 0; i < rightSide.length(); i++) {
            stringBuilder.append(getRCode(rightSide.charAt(i)));
        }

        return stringBuilder.toString();
    }
    private String getControlNum(String barcode) {
        barcode = barcode.substring(0, 12);

        int evenSum = 0, oddSum = 0;

        for (int i = 0; i < barcode.length(); i++) {
            int num = Integer.parseInt(Character.toString(barcode.charAt(i)));

            if (i % 2 == 0) {
                oddSum += num;
            } else {
                evenSum += num;
            }
        }

        int result = (evenSum * 3 + oddSum) % 10;
        result = (result != 0) ? 10 - result: result;

        return Integer.toString(result);
    }
    public String barcodeToBytecode(String barcode) {
        return getGuard() + getLeftSideBytecode(barcode) + getCenterGuardCode() + getRightSideBytecode(barcode) + getGuard();
    }
}