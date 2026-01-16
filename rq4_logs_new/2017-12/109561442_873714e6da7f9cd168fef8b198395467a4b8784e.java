package com.gandazhi.sell.aspect;

import com.gandazhi.sell.common.RedisConstant;
import com.gandazhi.sell.common.RedisIndex;
import com.gandazhi.sell.customException.SellerAuthorizeException;
import com.gandazhi.sell.util.CookieUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;
import javax.servlet.http.Cookie;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import redis.clients.jedis.Jedis;

import javax.servlet.http.HttpServletRequest;

/**
 * @author gandazhi
 * @create 2017-12-07 下午8:54
 **/
@Aspect
@Component
@Slf4j
public class SellerAuthorizeAspect {

    @Pointcut("execution(public * com.gandazhi.sell.controller.Seller*.*(..))"+
    "&& !execution(public * com.gandazhi.sell.controller.SellerUserController.*(..))")
    public void verify(){}

    @Before("verify()")
    public void doVerify(){
        ServletRequestAttributes requestAttributes = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes());
        HttpServletRequest request = requestAttributes.getRequest();

        //查询cookie
        Cookie cookie = CookieUtil.get(request, "token");
        if (cookie == null){
            log.warn("[登录校验],Cookie中找不到token");
            throw new SellerAuthorizeException();
        }

        //去redis中查找
        Jedis jedis = new Jedis();
        jedis.select(RedisIndex.SESSION.getCode());
        String tokenValue = jedis.get(String.format(RedisConstant.TOKEN_PREFIX, cookie.getValue()));
        if (tokenValue == null || StringUtils.isEmpty(tokenValue)){
            log.warn("[登录校验],redis中找不到session");
            throw new SellerAuthorizeException();
        }
    }
}