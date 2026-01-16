/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: EntityController
 * Author:   wangcl
 * Date:     2018/12/6 下午2:52
 * Description: 基础
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.njty.tydp.commom;

import com.njty.tydp.model.MsgModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 〈一句话功能简述〉<br> 
 * 〈基础〉
 *
 * @author wangcl
 * @create 2018/12/6
 * @since 1.0.0
 */
@RestController
public abstract class EntityController {

    protected static final Logger logger = LoggerFactory.getLogger(EntityController.class);

    /**
     * 增加
     * @param map
     * @return
     */
    @RequestMapping("/add")
    public MsgModel add(@RequestParam Map<String,Object> map) {
        MsgModel msgModel = new MsgModel();
        try{
            msgModel.setSuccessData(getEntityService().add(map));
        }catch (Exception e){
            logger.info("EntityController add error :{}", e);
        }
        return msgModel;
    }


    /**
     * 修改
     * @param map
     * @return
     */
    @RequestMapping("/modify")
    public MsgModel modify(@RequestParam Map<String,Object> map) {
        MsgModel msgModel = new MsgModel();
        try{
            msgModel.setSuccessData(getEntityService().modify(map));
        }catch (Exception e){
            logger.info("EntityController modify error :{}", e);
        }
        return msgModel;
    }



    /**
     * 删除
     * @param map
     * @return
     */
    @RequestMapping("/del")
    public MsgModel delete(@RequestParam Map<String,Object> map) {
        MsgModel msgModel = new MsgModel();
        try{
            msgModel.setSuccessData(getEntityService().delete(map));
        }catch (Exception e){
            logger.info("EntityController delete error :{}", e);
        }
        return msgModel;
    }

    /**
     * 查询
     * @param map
     * @return
     */
    @RequestMapping("/findList")
    public MsgModel findList(@RequestParam Map<String,Object> map) {
        MsgModel msgModel = new MsgModel();
        try{
            msgModel = getEntityService().findList(map);
        }catch (Exception e){
            logger.info("EntityController findList error :{}", e);
        }
        return msgModel;
    }

    public abstract EntityService getEntityService();

}