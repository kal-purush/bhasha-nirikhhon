package bloomfiter;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;

/**
 * 用来存储位运算的逻辑结构，具体的数据存储地方需要实现对应的抽象方法
 */
public abstract class AbstractBitmaps<T> {
    private static final String DEFUALT_BASE_KEY = "bloomfilter";
    private static final String DEFUALT_CURSOR = "cursor";
    protected long bitSize;  // 位数组的长度
    protected String baseKey ;  // 用来区分不同位数组的key值，分组ID

    /**
     * 通过待插入的估算条数，转化成存储位数字的长度
     * @param bits
     */
    public void setBits(long bits){
        // 通过位长度，再结合操作系统的位长度，计算出 真实位数组的长度
        this.bitSize = LongMath.divide(bits, 64, RoundingMode.CEILING) * Long.SIZE;//位数组的长度，相当于n个long的长度
        if (getBitCount() == 0) {
            this.setBitValue(currentKey(), bitSize - 1, false);
        }
    }

    public void setBaseKey(String baseKey) {
        this.baseKey = baseKey;
    }

    public String getBaseKey() {
        return Objects.isNull(this.baseKey) ? DEFUALT_BASE_KEY:this.baseKey ;
    }

    boolean get(long[] offsets) {
        for (long i = 0; i < cursor() + 1; i++) {
            final long cursor = i;
            //只要有一个cursor对应的bitmap中，offsets全部命中，则表示可能存在
            boolean match = Arrays.stream(offsets).boxed()
                    .map(  offset -> this.getBitValue(genkey(cursor), offset) )
                    .allMatch(b -> (Boolean) b);
            if (match)
                return true;

        }
        return false;
    }
    /*
     * 查询某个偏移量是否存在
     */
    boolean get(final long offset) {
        return this.getBitValue(currentKey(), offset);
    }

    /**
     * 批量设置偏移量的值
     * @param offsets
     * @return
     */
    boolean set(long[] offsets) {
        if (cursor() > 0 && get(offsets)) {
            return false;
        }
        boolean bitsChanged = false;
        for (long offset : offsets)
            bitsChanged |= set(offset);
        return bitsChanged;
    }
    /**
     * 设置某个偏移量的值
     */
    boolean set(long offset) {
        if (!get(offset)) {
            this.setBitValue(currentKey(), offset, true);
            return true;
        }
        return false;
    }
    /**
     * 获取某个位的为1的个数
     */
    private long getBitCount() {
        return this.bitCount(currentKey());
    }

    long bitSize() {
        return this.bitSize;
    }

    private String currentKey() {
        return genkey(cursor());
    }

    private String genkey(long cursor) {
        return getBaseKey() + "-" + cursor;
    }

    private String currentCursor() {
        return getBaseKey() + "-" + DEFUALT_CURSOR;
    }

    private Long cursor() {
        String cursor = this.getString(this.currentCursor());
        if(Objects.isNull(cursor)){
            return 0l;
        }else {
            return Long.valueOf(cursor);
        }

    }

    public void ensureCapacityInternal() {
        // 位数组中已经存储了超过了一半的量时，就进行扩容，有点简单粗暴
        if (getBitCount() * 2 > bitSize()){
            grow();
        }

    }

    /**
     * 扩容，针对一个数组无法存储下整个数据量，则需要分成多段进行存储
     */
    public void grow() {
        Long cursor = this.incr(this.currentCursor());
        this.setBitValue(genkey(cursor),bitSize - 1, false);
    }

    /**
     * 重置，就是清除所有的存储中的数据
     */
    public void reset() {
        String[] keys = LongStream.range(0, cursor() + 1).boxed().map(this::genkey).toArray(String[]::new);
        this.deleteForKeys(Arrays.asList(keys));
        this.set(this.currentCursor(), "0");
        this.setBitValue(currentKey(), bitSize - 1, false);

    }

    public abstract void setRedis(T t);
    abstract void setBitValue(String key, long offset, boolean value);
    abstract Boolean getBitValue(String key, long offset);
    abstract Long incr(String key);
    abstract String getString(String key);
    abstract long bitCount(String key);
    abstract void deleteForKeys(List<String> keys);
    abstract void set(String key,String value);

}