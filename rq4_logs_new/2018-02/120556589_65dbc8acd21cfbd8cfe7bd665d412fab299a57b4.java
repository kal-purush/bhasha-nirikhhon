package com.zwk.tool.exceller.unit;

import com.zwk.tool.exceller.service.UnmarshallerService;
import com.zwk.tool.exceller.service.UnmarshallerServiceImpl;
import com.zwk.tool.exceller.util.Fixture;
import com.zwk.tool.exceller.util.object.Example;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class UnmarshallerServiceImplTest {

    private static final List<Example> expectedExamples;

    static {
        Example example1 = new Example(new Date(2017-1900, 7, 11), 2, "1.25");
        Example example2 = new Example(new Date(2017-1900, 10, 11, 2, 52, 48), 1, "43050.12");
        expectedExamples = Arrays.asList(example1, example2);
    }

    @Test
    public void fromExcelSuccess () throws Exception {

        InputStream file = Fixture.getFile("excel/test1.xlsx");

        UnmarshallerService unmarshallerService = new UnmarshallerServiceImpl();
        List<Example> examples = unmarshallerService.fromExcel(file, Example.class);


        Assert.assertEquals(expectedExamples, examples);

    }
}