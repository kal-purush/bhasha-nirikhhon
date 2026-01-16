package String;

public class LicenseKeyFormatting {
    public String licenseKeyFormatting(String S, int K) {
        S = S.replace("-", "").toUpperCase();
        int length = S.length();
        if (length == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        int first = length % K == 0 ? K : length % K;

        sb.append(S, 0, first);

        int part = first == K ? length / K : length / K + 1;

        for (int i = 1; i < part; i++) {
            sb.append("-");
            sb.append(S, first + (i - 1) * K, first + i * K);
        }

        return sb.toString();
    }
}