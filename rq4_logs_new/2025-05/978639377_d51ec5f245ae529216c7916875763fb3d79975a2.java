package com.spring.aop.framework.autoproxy;

import com.spring.aop.PointcutAdvisor;
import com.spring.aop.framework.AopProxyFactory;
import com.spring.aop.framework.DefaultAopProxyFactory;
import com.spring.aop.framework.ProxyFactoryBean;
import com.spring.beans.BeanException;
import com.spring.beans.factory.BeanFactory;
import com.spring.beans.factory.config.BeanPostProcessor;
import com.spring.util.PatternMatchUtils;

/**
 * 根据bean名称自动创建代理
 *
 * @author zhenxingchen4
 * @since 2025/5/14
 */
public class BeanNameAutoProxyCreator implements BeanPostProcessor {
    private String pattern;
    private BeanFactory beanFactory;
    private AopProxyFactory aopProxyFactory;
    private String interceptorName;
    private PointcutAdvisor advisor;

    public BeanNameAutoProxyCreator() {
        this.aopProxyFactory = new DefaultAopProxyFactory();
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public void setAopProxyFactory(AopProxyFactory aopProxyFactory) {
        this.aopProxyFactory = aopProxyFactory;
    }

    public void setInterceptorName(String interceptorName) {
        this.interceptorName = interceptorName;
    }

    public void setAdvisor(PointcutAdvisor advisor) {
        this.advisor = advisor;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeanException {
        if (isMatch(beanName, this.pattern)) {
            ProxyFactoryBean proxyFactoryBean = new ProxyFactoryBean();
            proxyFactoryBean.setAopProxyFactory(this.aopProxyFactory);
            proxyFactoryBean.setBeanFactory(this.beanFactory);
            proxyFactoryBean.setInterceptorName(this.interceptorName);
            proxyFactoryBean.setTarget(bean);
            proxyFactoryBean.setTargetName(beanName);
            return proxyFactoryBean;
        }

        return bean;
    }

    private boolean isMatch(String beanName, String mappedName) {
        return PatternMatchUtils.simpleMatch(mappedName, beanName);
    }


    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeanException {
        return null;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }
}