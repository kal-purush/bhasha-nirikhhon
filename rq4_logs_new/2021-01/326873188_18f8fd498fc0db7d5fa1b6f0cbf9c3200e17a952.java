package com.fly.data.sync.aspect;

import com.fly.data.sync.annotation.SyncLock;
import com.fly.data.sync.entity.DataModel;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * @author guoxiang
 * @version 1.0.0
 * @since 2021/1/6
 */
@Component
@Aspect
@Slf4j
public class SyncLockAspect {


    @Around("@annotation(syncLock)")
    public <T> Object check(ProceedingJoinPoint joinPoint, SyncLock syncLock) throws Throwable {

        Object[] args = joinPoint.getArgs();
        DataModel<T> model = getDataModel(args);

        model.getDataLock().lock();
        log.info("- ====== get lock of model {} success...", model.getTable());

        Object result;
        try {
            result = joinPoint.proceed(args);
        } finally {
            model.getDataLock().unlock();
            log.info("- ====== release lock of model {} success...", model.getTable());
        }

        return result;
    }


    @SuppressWarnings("unchecked")
    public <T> DataModel<T> getDataModel(Object[] args) {
        return (DataModel<T>) Arrays.stream(args)
                .filter(arg -> arg instanceof DataModel)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("cannot get data model"));
    }
}