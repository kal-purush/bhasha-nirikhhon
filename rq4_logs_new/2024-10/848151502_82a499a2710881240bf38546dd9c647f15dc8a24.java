package com.lucky.around.meal.datapipeline;

import java.util.concurrent.BlockingDeque;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class CollectService {

  private final DataQueue dataQueue;

  @Async
  public void collectData() {
    try {
      for (int i = 0; i < 10; i++) {
        // 데이터 수집하기
        Thread.sleep(1000);
        RawData rawData = new RawData("Raw Data " + i);
        log.info("수집: " + rawData.getData());

        // 수집한 결과를 큐에 담음
        BlockingDeque<RawData> collectQueue = dataQueue.getCollectQueue();
        collectQueue.put(rawData);
        log.info("수집 큐 크기: " + collectQueue.size());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}