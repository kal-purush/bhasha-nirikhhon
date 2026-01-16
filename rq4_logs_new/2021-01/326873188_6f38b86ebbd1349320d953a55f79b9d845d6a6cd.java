package com.fly.data.sync.service.impl;

import com.fly.data.sync.entity.DataModel;
import com.fly.data.sync.service.SyncDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class SyncDataServiceImpl implements SyncDataService {

    @Override
    public <T> void syncAll(DataModel<T> model) {
        log.info("- sync all data for model: {}", model.getTable());

        //获取数据 E&T
        List<T> dataList = extractAll(model);

        //加载到数据库 L
        loadAll(dataList, model);

        log.info("- finish sync all for model: {}", model.getTable());
    }



    @Override
    public <T> void syncDelta(DataModel<T> model, Message message) {
        log.info("- sync delta for model: {}", model.getTable());



        log.info("- finish sync delta for model: {}", model.getTable());
    }


    private <T> List<T> extractAll(DataModel<T> model) {
        return null;
    }

    private <T> void loadAll(List<T> dataList, DataModel<T> model) {

    }
}