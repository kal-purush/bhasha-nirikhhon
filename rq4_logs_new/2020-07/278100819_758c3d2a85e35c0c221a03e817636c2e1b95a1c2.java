package correcter;

public class DefaultCoding {
    public static String decodeToBinary(String binaryBytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < binaryBytes.length(); i += 8) {
            int b1, b2, b3, parity;
            if (binaryBytes.charAt(i) != binaryBytes.charAt(i + 1)) {
                b2 = char0or1toInt(binaryBytes.charAt(i + 2));
                b3 = char0or1toInt(binaryBytes.charAt(i + 4));
                parity = char0or1toInt(binaryBytes.charAt(i + 6));
                b1 = (b3 + b2) % 2 == parity ? 0 : 1;
                sb.append(b1).append(b2).append(b3);
                continue;
            }
            b1 = char0or1toInt(binaryBytes.charAt(i));
            if (binaryBytes.charAt(i + 2) != binaryBytes.charAt(i + 3)) {
                b3 = char0or1toInt(binaryBytes.charAt(i + 4));
                parity = char0or1toInt(binaryBytes.charAt(i + 6));
                b2 = (b3 + b1) % 2 == parity ? 0 : 1;
                sb.append(b1).append(b2).append(b3);
                continue;
            }
            b2 = char0or1toInt(binaryBytes.charAt(i + 2));
            if (binaryBytes.charAt(i + 4) != binaryBytes.charAt(i + 5)) {
                parity = char0or1toInt(binaryBytes.charAt(i + 6));
                b3 = (b2 + b1) % 2 == parity ? 0 : 1;
                sb.append(b1).append(b2).append(b3);
                continue;
            }
            b3 = char0or1toInt(binaryBytes.charAt(i + 4));
            sb.append(b1).append(b2).append(b3);
        }
        return sb.toString();
    }

    public static String encode(String input) {
        int sum = 0;
        StringBuilder sb = new StringBuilder();
        input += "0".repeat(input.length() % 3 == 0 ? 0 : 3 - (input.length() % 3));
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            sb.append(c).append(c);
            if (c == '1') {
                sum++;
            }
            if (i % 3 == 2) {
                sb.append(sum % 2).append(sum % 2);
                sum = 0;
            }
        }
        return sb.toString();
    }

    private static int char0or1toInt (char c){
        if (c == '1') return 1;
        else if (c == '0') return 0;
        else throw new IllegalArgumentException();
    }
}