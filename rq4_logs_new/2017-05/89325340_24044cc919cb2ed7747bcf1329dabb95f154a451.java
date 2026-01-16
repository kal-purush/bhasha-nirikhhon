package store.StoreManager;

import com.github.lwhite1.tablesaw.api.ColumnType;
import com.github.lwhite1.tablesaw.api.Table;
import com.github.lwhite1.tablesaw.io.csv.CsvReader;
import com.github.lwhite1.tablesaw.store.StorageManager;
import org.junit.Test;

import java.nio.file.NoSuchFileException;

import static com.github.lwhite1.tablesaw.api.ColumnType.*;
import static com.github.lwhite1.tablesaw.api.ColumnType.CATEGORY;
import static com.github.lwhite1.tablesaw.api.ColumnType.FLOAT;
import static junit.framework.TestCase.assertEquals;

/**
 * Created by yikui on 2017/5/30.
 */
public class ForReadTable {

    @Test(expected = NoSuchFileException.class)
    public void TS_SM_01_01_001() throws Exception {
        Table t2 = StorageManager.readTable("testdata/test2.csv.saw");
    }

    @Test
    public void TS_SM_01_02_001() throws Exception {
        Table t1 = CsvReader.read(COLUMN_TYPES, "data/1950-2014_torn.csv");
        Table t2 = StorageManager.readTable("testdata/test1.csv.saw");
        assertEquals(t1.toString(), t2.toString());
    }

    private static final ColumnType[] COLUMN_TYPES = {
            FLOAT,   // number by year
            FLOAT,   // year
            FLOAT,   // month
            FLOAT,   // day
            LOCAL_DATE,  // date
            LOCAL_TIME,  // time
            CATEGORY, // tz
            CATEGORY, // st
            CATEGORY, // state fips
            FLOAT,    // state torn number
            FLOAT,    // scale
            FLOAT,    // injuries
            FLOAT,    // fatalities
            CATEGORY, // loss
            FLOAT,   // crop loss
            FLOAT,   // St. Lat
            FLOAT,   // St. Lon
            FLOAT,   // End Lat
            FLOAT,   // End Lon
            FLOAT,   // length
            FLOAT,   // width
            FLOAT,   // NS
            FLOAT,   // SN
            FLOAT,   // SG
            CATEGORY,  // Count FIPS 1-4
            CATEGORY,
            CATEGORY,
            CATEGORY
    };

}