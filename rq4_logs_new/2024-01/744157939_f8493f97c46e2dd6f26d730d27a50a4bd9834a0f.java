public class aes{

    public static void main(String[] args) {
        String K = "0100101011110101";
        String W0 = "01001010";
        String W1 = "11110101";

        System.out.println("Original Key (K): " + K);
        System.out.println("Round Key 0 (W0): " + W0);
        System.out.println("Round Key 1 (W1): " + W1);

        // Generate round keys for S-AES
        String[] roundKeys = generateRoundKeys(K, W0, W1);

        // Print the generated round keys
        for (int i = 0; i < roundKeys.length; i++) {
            System.out.println("Round Key " + (i + 2) + ": " + roundKeys[i]);
        }
    }

    public static String[] generateRoundKeys(String K, String W0, String W1) {
        String[] roundKeys = new String[4];

        // Round Key 2 (W2)
        roundKeys[0] = xorBinaryStrings(W0, keySchedule(W1));

        // Round Key 3 (W3)
        roundKeys[1] = xorBinaryStrings(W1, keySchedule(roundKeys[0]));

        // Round Key 4 (W4)
        roundKeys[2] = xorBinaryStrings(roundKeys[0], keySchedule(roundKeys[1]));

        // Round Key 5 (W5)
        roundKeys[3] = xorBinaryStrings(roundKeys[1], keySchedule(roundKeys[2]));

        return roundKeys;
    }

    public static String keySchedule(String input) {
        // Example key schedule implementation (rotate input to the left by 1 bit)
        return input.substring(1) + input.charAt(0);
    }

    public static String xorBinaryStrings(String a, String b) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < a.length(); i++) {
            result.append(a.charAt(i) ^ b.charAt(i));
        }
        return result.toString();
    }
}