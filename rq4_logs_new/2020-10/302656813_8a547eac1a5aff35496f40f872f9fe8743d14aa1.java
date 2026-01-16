package cn.xfakir.xmall.security.service;

import cn.xfakir.xmall.common.entity.XmMember;
import cn.xfakir.xmall.common.mapper.XmMemberAuthorizationMapper;
import cn.xfakir.xmall.common.mapper.XmMemberMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

public class XmUserDetailService implements UserDetailsService {

    @Autowired
    private XmMemberMapper xmMemberMapper;

    @Override
    public UserDetails loadUserByUsername(String s) throws UsernameNotFoundException {
        XmMember xmMember = xmMemberMapper.getMemberByMemberId(Long.valueOf(s));
        if (xmMember == null) {
            throw new UsernameNotFoundException("username cannot be found");
        }

        return new User();
    }
}