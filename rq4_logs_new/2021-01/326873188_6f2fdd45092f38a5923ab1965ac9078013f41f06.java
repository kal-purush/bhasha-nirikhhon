package com.fly.data.sync.listener;

import com.fly.data.sync.config.SyncDataConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class SyncDataListener {

    @EventListener(ApplicationReadyEvent.class)
    public void syncData() {
        log.info("- begin to sync data...");

        List<String> tableList = SyncDataConfig.getTableList();

        if (tableList.isEmpty()) {
            log.info("- table list is empty, return now...");
            return;
        }

        tableList.forEach(this::syncDataForTable);
    }


    /**
     * 同步指定表的数据
     *
     * @param tableName 表名称
     */
    private void syncDataForTable(String tableName) {


    }

}