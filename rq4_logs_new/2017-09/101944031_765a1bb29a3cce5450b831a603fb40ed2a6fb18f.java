package cn.shoppingmall.bean;

/**
 * Created by ${易淼} on 2017/9/30.
 * 电话：15036145858
 * 邮箱：11058289@qq.com
 */

public class OrderCountBean {


    /**
     * success : true
     * msg : 返回成功
     * data : {"dataCount":1,"dataClosed":0,"dataNoPay":0,"dataPaid":0,"dataDelivered":0,"dataFinished":0}
     * errcode :
     */

    private String success;
    private String msg;
    private DataEntity data;
    private String errcode;

    public void setSuccess(String success) {
        this.success = success;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public void setData(DataEntity data) {
        this.data = data;
    }

    public void setErrcode(String errcode) {
        this.errcode = errcode;
    }

    public String getSuccess() {
        return success;
    }

    public String getMsg() {
        return msg;
    }

    public DataEntity getData() {
        return data;
    }

    public String getErrcode() {
        return errcode;
    }

    public static class DataEntity {
        /**
         * dataCount : 1
         * dataClosed : 0
         * dataNoPay : 0
         * dataPaid : 0
         * dataDelivered : 0
         * dataFinished : 0
         */

        private int dataCount;
        private int dataClosed;
        private int dataNoPay;
        private int dataPaid;
        private int dataDelivered;
        private int dataFinished;

        public void setDataCount(int dataCount) {
            this.dataCount = dataCount;
        }

        public void setDataClosed(int dataClosed) {
            this.dataClosed = dataClosed;
        }

        public void setDataNoPay(int dataNoPay) {
            this.dataNoPay = dataNoPay;
        }

        public void setDataPaid(int dataPaid) {
            this.dataPaid = dataPaid;
        }

        public void setDataDelivered(int dataDelivered) {
            this.dataDelivered = dataDelivered;
        }

        public void setDataFinished(int dataFinished) {
            this.dataFinished = dataFinished;
        }

        public int getDataCount() {
            return dataCount;
        }

        public int getDataClosed() {
            return dataClosed;
        }

        public int getDataNoPay() {
            return dataNoPay;
        }

        public int getDataPaid() {
            return dataPaid;
        }

        public int getDataDelivered() {
            return dataDelivered;
        }

        public int getDataFinished() {
            return dataFinished;
        }
    }
}