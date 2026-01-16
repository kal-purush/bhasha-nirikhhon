package com.six.jdcom.home.activity;

import java.util.List;

/**
 * Created by chentong on 2017/11/21.
 */

public class AddSelect {

    /**
     * msg : 请求成功
     * code : 0
     * data : [{"createtime":"2017-11-21T13:56:37","orderid":2586,"price":399,"status":0,"title":null,"uid":2565},{"createtime":"2017-11-21T14:01:01","orderid":2589,"price":35999,"status":0,"title":null,"uid":2565}]
     * page : 1
     */

    private String msg;
    private String code;
    private String page;
    private List<DataBean> data;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public List<DataBean> getData() {
        return data;
    }

    public void setData(List<DataBean> data) {
        this.data = data;
    }

    public static class DataBean {
        /**
         * createtime : 2017-11-21T13:56:37
         * orderid : 2586
         * price : 399
         * status : 0
         * title : null
         * uid : 2565
         */

        private String createtime;
        private int orderid;
        private String price;
        private int status;
        private Object title;
        private int uid;

        public String getCreatetime() {
            return createtime;
        }

        public void setCreatetime(String createtime) {
            this.createtime = createtime;
        }

        public int getOrderid() {
            return orderid;
        }

        public void setOrderid(int orderid) {
            this.orderid = orderid;
        }

        public String getPrice() {
            return price;
        }

        public void setPrice(String price) {
            this.price = price;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public Object getTitle() {
            return title;
        }

        public void setTitle(Object title) {
            this.title = title;
        }

        public int getUid() {
            return uid;
        }

        public void setUid(int uid) {
            this.uid = uid;
        }
    }
}