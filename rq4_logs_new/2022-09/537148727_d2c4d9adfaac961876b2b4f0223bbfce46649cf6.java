package modules.config;

import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import modules.util.JwtUtil;
import org.apache.http.HttpRequest;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.shiro.authc.AuthenticationToken;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * @description:
 * @author:kangshizhu
 * @create:2022/9/1823:53
 **/
@Slf4j
@Component
 class MybastisColumnsHandler implements MetaObjectHandler {

    @Resource
    HttpServletRequest httpRequest;




    //设置数据新增时候的，字段自动赋值规则
    @SneakyThrows
    @Override
    public void insertFill(MetaObject metaObject) {
        log.info("开始创建");
        String token=httpRequest.getHeader("X-Access-Token");
        Long userId=JwtUtil.getUserId(token);
        this.setInsertFieldValByName("createTime", LocalDateTime.now(),metaObject);
        this.setInsertFieldValByName("createBy",userId,metaObject);
        this.setInsertFieldValByName("updateTime", LocalDateTime.now(),metaObject);
        this.setInsertFieldValByName("updateBy", userId,metaObject);

    }



    //设置数据修改update时候的，字段自动赋值规则
    @Override
    public void updateFill(MetaObject metaObject) {
        log.info("开始修改");
        String token=httpRequest.getHeader("X-Access-Token");
        Long userId=JwtUtil.getUserId(token);
        this.setFieldValByName("updateTime",LocalDateTime.now(),metaObject);
        this.setFieldValByName("updateBy",userId,metaObject);
    }
}