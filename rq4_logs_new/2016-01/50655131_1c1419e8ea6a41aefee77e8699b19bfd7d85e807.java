package biz.base.service;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import common.constants.ResponseCode;
import common.utils.IntegerUtils;

import exception.BusinessException;
import extend.jdbc.BaseDao;

/**
 * 通过Service公共抽象类
 * @author Joeson Chan<chenxuegui.cxg@alibaba-inc.com>
 * @since 2015年10月23日
 *
 */
public abstract class BaseService extends BaseDao{
    
    public static <T> boolean save(T t){
        checkNotNull(t, "保存实体不能为空");
        
        return BaseDao.add(t);
    }
    
    public static <T> boolean saveMuti(List<T> list){
        checkFalse(CollectionUtils.isNotEmpty(list), "保存列表不能为空");
        
        return BaseDao.addMuti(list);
    }
    
//    public static <T> boolean replace(Class<T> clazz, T t){
//        checkNotNull(t, "保存实体不能为空");
//        
//        if(null != t.getIdValue()){
//            T old = findById(clazz, IntegerUtil.parseInt(t.getIdValue().toString()));
//            BeanCopy.copy(old, t, true);
//            return AbstractDao.replace(old) > 0;
//        }else{
//            return AbstractDao.replace(t) > 0;
//        }
//    }

    public static <T> T findById(Integer id, Class<T> clazz) {
        if (null == clazz || null == id) {
            return null;
        }

        return BaseDao.findById(id, clazz);
    }

    public static <T> boolean update(T t) {
        if (null == t) {
            return false;
        }

        return BaseDao.update(t);
    }

    public static <T> boolean delete(Class<T> clazz, Integer id){
        if(IntegerUtils.nullOrZero(id)){
            return false;
        }
        
        return BaseDao.deleteById(clazz, id);
    }

    protected static <T> boolean checkNotNull(T t) {
        return null != t ? true : false;
    }

    protected static <T> T checkNotNull(T t, String msg) {
        if (checkNotNull(t)) {
            return t;
        } else {
            throw new BusinessException(ResponseCode.PARAM_ILLEGAL, msg);
        }
    }

    /**
     * t 为null或false返回true， t为tue返回false
     * @param t
     * @param msg
     */
    protected static boolean checkFalse(Boolean t) {
        return t == null || t == false ? true : false;
    }

    /**
     * true 时候抛出异常
     * @param t
     * @param msg
     */
    protected static boolean checkFalse(Boolean t, String msg) {
        if (checkFalse(t)) {
            return true;
        } else {
            throw new BusinessException(ResponseCode.PARAM_ILLEGAL, msg);
        }
    }

}