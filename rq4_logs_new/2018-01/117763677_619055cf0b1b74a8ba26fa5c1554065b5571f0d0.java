package com.ssh.PageBean;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * Created by dllo on 2017/8/6.
 */
public class PageBean<T> {

    // 必选项
    private int pageNum;            //当前页 -- 浏览器传递
    private int pageSize;         //每页显示个数 -- 固定值（浏览器传递）
    private int totalRecord;        //总记录数（数据库总条数） -- 查询数据库获得
    // 计算项
    private int startIndex;            //开始索引
    private int totalPage;            //总分页数
    // 分页数据
    private List<T> beanList;            //分页数据 --查询数据库获得
    // 导航动态显示条
    private int begin;                //循环开始
    private int end;                //循环结束

//    private String url;  // 它就是url后的条件！


    public PageBean(int pageNum, int pageSize, int totalRecord) {
        super();
        this.pageNum = pageNum;
        this.pageSize = pageSize;
        this.totalRecord = totalRecord;

        // 0 优化 -- 排除负页
        if (this.pageNum < 1) {
            this.pageNum = 1;
        }

        //1 计算项

        // 1.2 总分页数
        this.totalPage = (this.totalRecord + this.pageSize - 1) / this.pageSize;
         /*优化--排除页码超出范围*/
        if (this.pageNum>totalPage){
            this.pageNum=totalPage;
        }
        // 1.1  开始索引
        this.startIndex = (this.pageNum - 1) * this.pageSize;

        //2 动态条 最多显示10分页，前5后4
        // 2.1 默认值
        this.begin = 1;
        this.end = 10;
        // 2.2 处理
        // * 假设 totalPage = 4;
        // 如果总页数不足10页，那么把所有的页数都显示出来
        if (this.totalPage <= 10) {
            this.end = this.totalPage;
        } else {

            // 假设 totalPage = 35
            // * 前5后4
            // <%-- 当总页数>10时，通过公式计算出begin和end --%>
            this.begin = this.pageNum - 5;
            this.end = this.pageNum + 4;

            // <%-- 头溢出 --%>
            // * pageNum = 1
            if (this.begin < 1) {
                this.begin = 1;
                this.end = 10;
            }

            // <%-- 尾溢出 --%>
            // * pageNUm = 35
            if (this.end > this.totalPage) {
                this.end = this.totalPage;
                this.begin = this.totalPage - 9;
            }

        }

    }

    public int getTotalPage() {
        return totalPage;
    }


    public List<T> getBeanList() {
        return beanList;
    }

    public void setBeanList(List<T> beanList) {
        this.beanList = beanList;
    }

    public int getBegin() {
        return begin;
    }

    public void setBegin(int begin) {
        this.begin = begin;
    }

    public void setTotalPage(int totalPage) {
        this.totalPage = totalPage;
    }

    public int getPageNum() {
        return pageNum;
    }

    public void setPageNum(int pageNum) {
        this.pageNum = pageNum;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getTotalRecord() {
        return totalRecord;
    }

    public void setTotalRecord(int totalRecord) {
        this.totalRecord = totalRecord;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }


    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }
}