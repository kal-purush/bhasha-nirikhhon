package com.autohome.mail.service.impl;

import com.autohome.common.email.MailUtil;
import com.autohome.common.util.JedisCore;
import com.autohome.common.vo.R;
import com.autohome.entity.User;
import com.autohome.mail.config.MyMailUtil;
import com.autohome.mail.dao.MailDao;
import com.autohome.mail.service.MailService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class MailServciceImpl implements MailService {

//    String str = MailUtil.generateRandomCode(6);
//    //生成随机6位的字符


    //将验证码放入redis中，设置时间为5分钟；
    @Autowired
    private MailDao dao;

    @Override
    public R sendCodeMail(String to,String str) {

            MailUtil.sendMail(to, str);
            return R.ok("验证码已经发送，请注意查看您的邮箱，验证码5分钟有效。");
    }
    //邮箱注册服务，发送验证码服务。

    @Override
    public R sendUserMail(String to,String name,String passWord) {
        MyMailUtil.sendMail(to,name,passWord);
        return R.ok("已经将您的账号密码发送到您的邮箱中，请及时完善您的个人信息");
    }
    //邮箱注册服务，发送账号密码。

}