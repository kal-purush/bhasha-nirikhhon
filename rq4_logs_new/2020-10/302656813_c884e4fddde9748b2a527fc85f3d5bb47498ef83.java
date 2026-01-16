package cn.xfakir.xmall.portal.controller;

import cn.xfakir.xmall.common.entity.XmMember;
import cn.xfakir.xmall.common.mapper.XmMemberMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PortalController {
    @Autowired
    private XmMemberMapper memberMapper;

    @RequestMapping("/test")
    public XmMember test() {
        System.out.println("test");
        XmMember member =  memberMapper.selectByPrimaryKey(1L);
        System.out.println(member);
        return member;
    }
}