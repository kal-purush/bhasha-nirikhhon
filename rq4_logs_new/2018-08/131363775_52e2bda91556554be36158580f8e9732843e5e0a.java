package me.chin.utils;


import java.util.regex.Pattern;

/**
 * Created by Allen on 2018/6/26.
 */
public class IpUtils {
    public static long ip2int(String ip) {
        if (ip == null) {
            throw new IllegalArgumentException(" ip can't be null");
        }
        String reg = "(\\d{1,3}\\.){3}\\d{1,3}";
        if (!Pattern.matches(reg, ip)) {
            throw new IllegalArgumentException(String.format("ip is not legal for %s", ip));
        }
        String[] splits = ip.split("\\.");
        long ip1 = Long.parseLong(splits[0]);
        int ip2 = Integer.parseInt(splits[1]);
        int ip3 = Integer.parseInt(splits[2]);
        int ip4 = Integer.parseInt(splits[3]);
        if ((ip1 < 1 || ip1 > 255) || (ip2 < 0 || ip2 > 255) || (ip3 < 0 || ip3 > 255) || (ip4 < 0 || ip4 > 255)){
            throw new IllegalArgumentException(String.format("ip is not legal for %s", ip));
        }
        long result = 0L;
        result |= ip4;
        result |= (ip3 << 8);
        result |= (ip2 << 16);
        result |= (ip1 << 24);
        return result;
    }

    public static String int2ip(long ip) {
        if (ip < 1 || ip >= (1L << 32)){
            throw new IllegalArgumentException(String.format("ip is not legal for %s",ip));
        }
        StringBuffer sb = new StringBuffer();
        sb.append(ip >> 24).append('.').append((ip >> 16) & 0xff).append('.').
                append((ip >> 8) & 0xff).append('.').append(ip & 0xff);
        return sb.toString();
    }
}