package tech.tablesaw.columns.numbers;

import com.google.common.collect.Lists;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.columns.AbstractColumnParser;
import tech.tablesaw.io.ReadOptions;
import tech.tablesaw.util.StringUtils;

public class NumberParser<T extends Number> extends AbstractColumnParser<T> {

    private final boolean ignoreZeroDecimal;

    public NumberParser(ColumnType columnType) {
        super(columnType);
        ignoreZeroDecimal = ReadOptions.DEFAULT_IGNORE_ZERO_DECIMAL;
    }

    public NumberParser(ColumnType columnType, ReadOptions readOptions) {
        super(columnType);
        if (readOptions.missingValueIndicators().length > 0) {
            missingValueStrings = Lists.newArrayList(readOptions.missingValueIndicators());
        }
        ignoreZeroDecimal = readOptions.ignoreZeroDecimal();
    }

    @Override
    public boolean canParse(String str) {
        if (isMissing(str)) {
            return true;
        }
        String s = str;
        try {
            if (ignoreZeroDecimal) {
                s = StringUtils.removeZeroDecimal(s);
            }
            parseNumber(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public T parse(String s) {
        return parseNumber(s);
    }

    @Override
    public double parseDouble(String s) {
        return parseNumber(s).doubleValue();
    }

    private T parseNumber(String str) {
        if (isMissing(str)) {
            return columnType().missingValueIndicator();
        }
        String s = str;
        if (ignoreZeroDecimal) {
            s = StringUtils.removeZeroDecimal(s);
        }
        return parseValue(AbstractColumnParser.remove(s, ','));
    }

    private T parseValue(String str) {
        // Perform actual parsing logic based on the specific number type (e.g., Integer, Long, Short)
        // Add your parsing logic here for the respective number type
        // ...
    }
}

class IntParser extends NumberParser<Integer> {

    public IntParser(ColumnType columnType) {
        super(columnType);
    }

    public IntParser(ColumnType columnType, ReadOptions readOptions) {
        super(columnType, readOptions);
    }

    // Add any additional methods or code specific to the IntParser class here
    // ...
}

class LongParser extends NumberParser<Long> {

    public LongParser(ColumnType columnType) {
        super(columnType);
    }

    public LongParser(ColumnType columnType, ReadOptions readOptions) {
        super(columnType, readOptions);
    }

    // Add any additional methods or code specific to the LongParser class here
    // ...
}

class ShortParser extends NumberParser<Short> {

    public ShortParser(ColumnType columnType) {
        super(columnType);
    }

    public ShortParser(ColumnType columnType, ReadOptions readOptions) {
        super(columnType, readOptions);
    }

    // Add any additional methods or code specific to the ShortParser class here
    // ...
}