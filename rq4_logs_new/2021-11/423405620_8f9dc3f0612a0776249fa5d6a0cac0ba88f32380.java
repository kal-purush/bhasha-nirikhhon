package ruangong.our_land.model.helper;

/**
 * 辅助类
 * @author wizardk
 * @email ozx1341530199@gmail.com
 */
public final class ObjectHelper {

    /**
     * 非空判断
     * @param object
     * @param <T>
     * @return
     */
    public static <T> T requireNonNull(T object) {
        if (object == null) {
            throw new NullPointerException("the object is null");
        }
        return object;
    }

    /**
     * 非空判断
     * @param object
     * @param name
     * @param <T>
     * @return
     */
    public static <T> T requireNonNull(T object, String name) {
        if (object == null) {
            throw new NullPointerException("the \" " + name + " \" is null");
        }
        return object;
    }

    /**
     * 判断value是否大于等于0
     * @param value
     * @param paramName
     * @return
     */
    public static int verifyPositive(int value, String paramName) {
        if (value <= 0) {
            throw new IllegalArgumentException(paramName + " > 0 required but it was " + value);
        }
        return value;
    }

    /**
     * 判断value是否大于0
     * @param value
     * @param paramName
     * @return
     */
    public static int verifyNonZeroPositive(int value, String paramName) {
        if (value < 0) {
            throw new IllegalArgumentException(paramName + " >= 0 required but it was " + value);
        }
        return value;
    }
}