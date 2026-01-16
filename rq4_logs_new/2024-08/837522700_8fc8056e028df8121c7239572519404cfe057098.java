package top.codingshen.middleware.dynamic.thread.pool.sdk.domain;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import top.codingshen.middleware.dynamic.thread.pool.sdk.domain.model.entity.ThreadPoolConfigEntity;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @ClassName DynamicThreadPoolService
 * @Description 动态线程池服务实现类
 * @Author alex_shen
 * @Date 2024/8/3 - 21:04
 */
@Slf4j
public class DynamicThreadPoolService implements IDynamicThreadPoolService {

    private final String applicationName;
    private final Map<String, ThreadPoolExecutor> threadPoolExecutorMap;

    public DynamicThreadPoolService(String applicationName, Map<String, ThreadPoolExecutor> threadPoolExecutorMap) {
        this.applicationName = applicationName;
        this.threadPoolExecutorMap = threadPoolExecutorMap;
    }

    /**
     * 查询线程池列表
     *
     * @return 线程池列表
     */
    @Override
    public List<ThreadPoolConfigEntity> queryThreadPoolList() {
        Set<String> threadPoolBeanNames = threadPoolExecutorMap.keySet();
        List<ThreadPoolConfigEntity> threadPoolVOS = new ArrayList<>(threadPoolBeanNames.size());

        threadPoolBeanNames.forEach(beanName -> {
            ThreadPoolExecutor threadPoolExecutor = threadPoolExecutorMap.get(beanName);
            ThreadPoolConfigEntity threadPoolConfigVO = ThreadPoolConfigEntity.builder()
                    .appName(applicationName)
                    .threadPoolName(beanName)
                    .corePoolSize(threadPoolExecutor.getCorePoolSize())
                    .maximumPoolSize(threadPoolExecutor.getMaximumPoolSize())
                    .poolSize(threadPoolExecutor.getPoolSize())
                    .activeCount(threadPoolExecutor.getActiveCount())
                    .queueSize(threadPoolExecutor.getQueue().size())
                    .queueType(threadPoolExecutor.getQueue().getClass().getName())
                    .remainingCapacity(threadPoolExecutor.getQueue().remainingCapacity())
                    .build();
            threadPoolVOS.add(threadPoolConfigVO);
        });

        return threadPoolVOS;
    }

    /**
     * 根据线程池名称查询线程池
     *
     * @param threadPoolName 线程池名称
     * @return 线程池
     */
    @Override
    public ThreadPoolConfigEntity queryThreadPoolByName(String threadPoolName) {
        if (threadPoolExecutorMap.containsKey(threadPoolName)) {
            ThreadPoolExecutor threadPoolExecutor = threadPoolExecutorMap.get(threadPoolName);
            return ThreadPoolConfigEntity.builder()
                    .appName(applicationName)
                    .threadPoolName(threadPoolName)
                    .corePoolSize(threadPoolExecutor.getCorePoolSize())
                    .maximumPoolSize(threadPoolExecutor.getMaximumPoolSize())
                    .poolSize(threadPoolExecutor.getPoolSize())
                    .activeCount(threadPoolExecutor.getActiveCount())
                    .queueSize(threadPoolExecutor.getQueue().size())
                    .queueType(threadPoolExecutor.getQueue().getClass().getName())
                    .remainingCapacity(threadPoolExecutor.getQueue().remainingCapacity())
                    .build();
        }
        return null;
    }

    /**
     * 更新线程池
     *
     * @param threadPoolConfigEntity 线程池配置实体
     */
    @Override
    public void updateThreadPool(ThreadPoolConfigEntity threadPoolConfigEntity) {
        if (threadPoolConfigEntity == null || !applicationName.equals(threadPoolConfigEntity.getAppName()))
            return;

        if (threadPoolExecutorMap.containsKey(threadPoolConfigEntity.getThreadPoolName())) {
            ThreadPoolExecutor threadPoolExecutor = threadPoolExecutorMap.get(threadPoolConfigEntity.getThreadPoolName());

            // 设置参数 [核心线程数 & 最大线程数]
            threadPoolExecutor.setCorePoolSize(threadPoolConfigEntity.getCorePoolSize());
            threadPoolExecutor.setMaximumPoolSize(threadPoolConfigEntity.getMaximumPoolSize());
        }
    }
}