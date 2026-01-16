package codingtests.integertoroman;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.TreeMap;

public class IntegerToRomanBasic {
    TreeMap<Integer, String> converterMap = new TreeMap<Integer, String>() {{
        put(1000, "M");
        put(900, "CM");
        put(500, "D");
        put(400, "CD");
        put(100, "C");
        put(90, "XC");
        put(50, "L");
        put(40, "XL");
        put(10, "X");
        put(9, "IX");
        put(5, "V");
        put(4, "IV");
        put(1, "I");
    }};

    public String convertIntegerToRoman(int nr) {
        Preconditions.checkArgument(nr>= 1 && nr <= 2000,"Number should be in range 1-2000");
        String romanNr = "";
        while (nr > 0) {
            Map.Entry<Integer, String> convertPair = converterMap.floorEntry(nr);
            int times = (int) (Math.floor(nr / convertPair.getKey()));
            romanNr += StringUtils.repeat(convertPair.getValue(), times);
            nr -= (convertPair.getKey() * times);
        }
        return romanNr;
    }
}