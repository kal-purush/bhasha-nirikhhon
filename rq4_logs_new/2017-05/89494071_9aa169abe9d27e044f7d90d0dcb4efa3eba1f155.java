package com.upc.model.dto;

import java.util.List;

/**
 * Created by Qloop on 2017/5/23.
 */
public class ReportListDto {


    private List<ReportBean> reportBeanList;
    private String result;

    public void setResult(String result) {
        this.result = result;
    }

    public String getResult() {
        return result;
    }

    public void setReportBeanList(List<ReportBean> reportBeanList) {
        this.reportBeanList = reportBeanList;
    }

    public List<ReportBean> getReportBeanList() {
        return reportBeanList;
    }

    public static class ReportBean {
        private long stuId;
        private String stuName;
        private String stuClass;

        public void setStuId(long stuId) {
            this.stuId = stuId;
        }

        public void setStuName(String stuName) {
            this.stuName = stuName;
        }

        public void setStuClass(String stuClass) {
            this.stuClass = stuClass;
        }

        public String getStuName() {
            return stuName;
        }

        public String getStuClass() {
            return stuClass;
        }

        public long getStuId() {
            return stuId;
        }
    }
}