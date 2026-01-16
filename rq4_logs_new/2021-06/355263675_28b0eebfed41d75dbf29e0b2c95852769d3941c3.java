package org.epam.data.test_data;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TestReportData {

    private String testSuiteName;
    private String total;
    private String passed;
    private String failed;
    private String skipped;
    private String productBug;
    private String autoBug;
    private String systemIssue;
    private String toInvestigate;
}