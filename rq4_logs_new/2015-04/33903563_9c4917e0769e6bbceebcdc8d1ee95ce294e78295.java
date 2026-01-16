package jerry.framework.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ���ڹ�����
 * Created by JerryLiu on 2015/4/17.
 */
public class DateUtils {    public static Date getDate() {
    return new Date(System.currentTimeMillis());
}

    public static String getDateString() {
        return new SimpleDateFormat("yyyy-MMddHH-mmss").format(getDate());
    }

    public static String getDateString(String pattern) {
        return new SimpleDateFormat(pattern).format(getDate());
    }
}