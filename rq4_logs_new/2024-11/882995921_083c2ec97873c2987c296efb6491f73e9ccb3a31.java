package com.reaplette.mypage.service;

import com.reaplette.domain.UserVO;
import com.reaplette.mypage.mappers.MyPageMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Log4j2
@RequiredArgsConstructor
@Transactional
public class MyPageService {

    private final MyPageMapper userMapper; // 자동 주입

    public UserVO getUser(String id) {
        log.info("getUser....." + id);
        UserVO user = userMapper.getUser(id);

        log.info(user);

        return user;
    }
}