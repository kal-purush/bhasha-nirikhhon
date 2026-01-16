package com.yundian.celebrity.bean;

import android.os.Parcel;
import android.os.Parcelable;

/**
 * Created by Administrator on 2017/8/30.
 */

public class FansAskBean implements Parcelable {


    /**
     * a_type : 2
     * answer_t : 0
     * ask_t : 1503972157
     * c_type : 0
     * headUrl : http://wx.qlogo.cn/mmopen/3Lqm1xHojtYnUQ1ic7CEE14diaNPRYbsfb07fubPCdrFRVufnHsDaulEOltibQ9NuHicZdecA3CFlVA63PGpVUhlSA/0
     * id : 9
     * nickName : BreakBlade
     * p_type : 1
     * purchased : 0
     * s_total : 0
     * sanswer : NULL
     * starcode : 1001
     * uask : 231
     * uid : 146
     * video_url :
     */

    private int a_type;
    private int answer_t;
    private int ask_t;
    private int c_type;
    private String headUrl;
    private int id;
    private String nickName;
    private int p_type;
    private int purchased;
    private int s_total;
    private String sanswer;
    private String starcode;
    private String uask;
    private int uid;
    private String video_url;

    public int getA_type() {
        return a_type;
    }

    public void setA_type(int a_type) {
        this.a_type = a_type;
    }

    public int getAnswer_t() {
        return answer_t;
    }

    public void setAnswer_t(int answer_t) {
        this.answer_t = answer_t;
    }

    public int getAsk_t() {
        return ask_t;
    }

    public void setAsk_t(int ask_t) {
        this.ask_t = ask_t;
    }

    public int getC_type() {
        return c_type;
    }

    public void setC_type(int c_type) {
        this.c_type = c_type;
    }

    public String getHeadUrl() {
        return headUrl;
    }

    public void setHeadUrl(String headUrl) {
        this.headUrl = headUrl;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public int getP_type() {
        return p_type;
    }

    public void setP_type(int p_type) {
        this.p_type = p_type;
    }

    public int getPurchased() {
        return purchased;
    }

    public void setPurchased(int purchased) {
        this.purchased = purchased;
    }

    public int getS_total() {
        return s_total;
    }

    public void setS_total(int s_total) {
        this.s_total = s_total;
    }

    public String getSanswer() {
        return sanswer;
    }

    public void setSanswer(String sanswer) {
        this.sanswer = sanswer;
    }

    public String getStarcode() {
        return starcode;
    }

    public void setStarcode(String starcode) {
        this.starcode = starcode;
    }

    public String getUask() {
        return uask;
    }

    public void setUask(String uask) {
        this.uask = uask;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public String getVideo_url() {
        return video_url;
    }

    public void setVideo_url(String video_url) {
        this.video_url = video_url;
    }


    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeInt(this.a_type);
        dest.writeInt(this.answer_t);
        dest.writeInt(this.ask_t);
        dest.writeInt(this.c_type);
        dest.writeString(this.headUrl);
        dest.writeInt(this.id);
        dest.writeString(this.nickName);
        dest.writeInt(this.p_type);
        dest.writeInt(this.purchased);
        dest.writeInt(this.s_total);
        dest.writeString(this.sanswer);
        dest.writeString(this.starcode);
        dest.writeString(this.uask);
        dest.writeInt(this.uid);
        dest.writeString(this.video_url);
    }

    public FansAskBean() {
    }

    protected FansAskBean(Parcel in) {
        this.a_type = in.readInt();
        this.answer_t = in.readInt();
        this.ask_t = in.readInt();
        this.c_type = in.readInt();
        this.headUrl = in.readString();
        this.id = in.readInt();
        this.nickName = in.readString();
        this.p_type = in.readInt();
        this.purchased = in.readInt();
        this.s_total = in.readInt();
        this.sanswer = in.readString();
        this.starcode = in.readString();
        this.uask = in.readString();
        this.uid = in.readInt();
        this.video_url = in.readString();
    }

    public static final Parcelable.Creator<FansAskBean> CREATOR = new Parcelable.Creator<FansAskBean>() {
        @Override
        public FansAskBean createFromParcel(Parcel source) {
            return new FansAskBean(source);
        }

        @Override
        public FansAskBean[] newArray(int size) {
            return new FansAskBean[size];
        }
    };
}