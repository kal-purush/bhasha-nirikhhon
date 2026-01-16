package top.codingshen.middleware.dynamic.thread.pool.sdk.trigger.listener;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.listener.MessageListener;
import top.codingshen.middleware.dynamic.thread.pool.sdk.domain.IDynamicThreadPoolService;
import top.codingshen.middleware.dynamic.thread.pool.sdk.domain.model.entity.ThreadPoolConfigEntity;
import top.codingshen.middleware.dynamic.thread.pool.sdk.registry.IRegistry;

import java.util.List;

/**
 * @ClassName ThreadPoolConfigAdjustListener
 * @Description 动态线程池变更监听
 * @Author alex_shen
 * @Date 2024/8/4 - 01:39
 */
@Slf4j
public class ThreadPoolConfigAdjustListener implements MessageListener<ThreadPoolConfigEntity> {

    private final IDynamicThreadPoolService dynamicThreadPoolService;
    private final IRegistry registry;

    public ThreadPoolConfigAdjustListener(IDynamicThreadPoolService dynamicThreadPoolService, IRegistry registry) {
        this.dynamicThreadPoolService = dynamicThreadPoolService;
        this.registry = registry;
    }

    @Override
    public void onMessage(CharSequence charSequence, ThreadPoolConfigEntity threadPoolConfigEntity) {
        log.info("动态线程池, 调整线程池配置, 线程池名称:{}, 核心线程数:{}, 最大线程数:{}", threadPoolConfigEntity.getThreadPoolName(), threadPoolConfigEntity.getCorePoolSize(), threadPoolConfigEntity.getMaximumPoolSize());
        dynamicThreadPoolService.updateThreadPool(threadPoolConfigEntity);

        // 更新后 上报最新数据
        List<ThreadPoolConfigEntity> threadPoolConfigEntities = dynamicThreadPoolService.queryThreadPoolList();
        registry.reportThreadPool(threadPoolConfigEntities);

        ThreadPoolConfigEntity threadPoolConfigEntityCurrent = dynamicThreadPoolService.queryThreadPoolByName(threadPoolConfigEntity.getThreadPoolName());
        registry.reportThreadPoolConfigParameter(threadPoolConfigEntityCurrent);

        log.info("动态线程池, 上报线程池配置:{}", JSON.toJSONString(threadPoolConfigEntityCurrent));
    }

}