package com.zhangqianyuan.teamwork.lostandfound.model;

import com.google.gson.Gson;
import com.zhangqianyuan.teamwork.lostandfound.bean.DynamicsRequestBean;
import com.zhangqianyuan.teamwork.lostandfound.bean.SearchBean;
import com.zhangqianyuan.teamwork.lostandfound.model.interfaces.IDynamicChildModel;

import io.reactivex.Observer;
import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.schedulers.Schedulers;

/**
 * Description
 * 动态  子Fragment model
 * @author zhoudada
 * @version $Rev$
 * @des
 * @updateAuthor $Author$
 * @updateDes
 */
public class DynamicChildModel extends BaseModel implements IDynamicChildModel {
    public DynamicChildModel(){
        super();
    }

    @Override
    public void getDynamicLostTodayData(DynamicsRequestBean dynamicsRequestBean, String session, Observer<SearchBean> observer) {
        api.getDynamicLostTodayData(new Gson().toJson(dynamicsRequestBean),session)
                .subscribeOn(Schedulers.io())
                .unsubscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(observer);
    }

    @Override
    public void getDynamicLostYesterdayData(DynamicsRequestBean dynamicsRequestBean, String session, Observer<SearchBean> observer) {
        api.getDynamicLostYesterdayData(new Gson().toJson(dynamicsRequestBean),session)
                .subscribeOn(Schedulers.io())
                .unsubscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(observer);
    }

    @Override
    public void getDynamicLostAgoData(DynamicsRequestBean dynamicsRequestBean, String session, Observer<SearchBean> observer) {
        api.getDynamicLostAgoData(new Gson().toJson(dynamicsRequestBean),session)
                .subscribeOn(Schedulers.io())
                .unsubscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(observer);
    }

    @Override
    public void getDynamicFindTodayData(DynamicsRequestBean dynamicsRequestBean, String session, Observer<SearchBean> observer) {
        api.getDynamicFindTodayData(new Gson().toJson(dynamicsRequestBean),session)
                .subscribeOn(Schedulers.io())
                .unsubscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(observer);
    }

    @Override
    public void getDynamicFindYesterdayData(DynamicsRequestBean dynamicsRequestBean, String session, Observer<SearchBean> observer) {
        api.getDynamicFindYesterdayData(new Gson().toJson(dynamicsRequestBean),session)
                .subscribeOn(Schedulers.io())
                .unsubscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(observer);
    }

    @Override
    public void getDynamicFindAgoData(DynamicsRequestBean dynamicsRequestBean, String session, Observer<SearchBean> observer) {
        api.getDynamicFindAgoData(new Gson().toJson(dynamicsRequestBean),session)
                .subscribeOn(Schedulers.io())
                .unsubscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(observer);
    }
}