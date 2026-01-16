package com.study.service.role;

import com.study.common.BaseRsp;
import com.study.common.IService;
import com.study.dao.role.RoleDao;
import com.study.message.role.req.AddRoleReq;
import com.study.service.cache.ResourceCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 添加角色服务类
 */
@Service("addRoleService")
public class AddRoleService implements IService<AddRoleReq, BaseRsp> {

    @Autowired
    private RoleDao roleDao;
    @Autowired
    private ResourceCache resourceCache;

    @Override
    public BaseRsp doExcute(AddRoleReq req) throws Exception {

        //新增角色
        roleDao.addRole(req.getDesc());

        //刷新本地缓存
        resourceCache.getResourceCache().refresh(req.getDesc());

        return new BaseRsp();
    }
}