package com.didispace.web;

import com.didispace.domain.User;
import com.didispace.mapper.AnnotationUserMapper;
import com.didispace.mapper.XmlUserMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Created by txc on 17-12-9.
 */
@RestController
public class TxTestController {

    @Autowired
    private AnnotationUserMapper annotationUserMapper;

    @Autowired
    private XmlUserMapper xmlUserMapper;

    /**
     * 测试TxAdviceInterceptor类是否生效
     * @return
     */
    @RequestMapping("tx/getAllTx")
    public String getAllTx() {
        List<User> userList = xmlUserMapper.getAll();
        userList.forEach((user)->{
            System.out.println(user.toString());
        });
        xmlUserMapper.getAll();
        return "success";
    }

    /**
     * 测试TxAdviceInterceptor类是否生效
     * @return
     */
    @RequestMapping("tx/addOneTx")
    public String addOneTx() throws Exception{
        xmlUserMapper.insert(User.newAUser(3005));
        boolean b = false;
        b = true;
        if(b){
            throw new Exception("");
        }
        xmlUserMapper.insert(User.newAUser(3006));
        return "success";
    }
}