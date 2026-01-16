package org.example.module.common.captcha.controller;

import com.google.common.base.Preconditions;
import com.wf.captcha.SpecCaptcha;
import com.wf.captcha.base.Captcha;
import org.example.common.constant.MsgKeyConstant;
import org.example.common.domain.response.Result;
import org.example.common.util.MessageUtil;
import org.example.module.common.captcha.domain.response.CaptchaResponse;
import org.example.module.common.captcha.enums.CaptchaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author linzhaoming
 * @since 2020-12-16
 **/
@RestController
@RequestMapping("/common/captcha")
public class CaptchaController {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @GetMapping
    public Result captcha(@RequestParam Integer type) {
        CaptchaType captchaType = CaptchaType.get(type);
        Preconditions.checkNotNull(captchaType, MessageUtil.get(MsgKeyConstant.CAPTCHA_TYPE_NOT_EXISTED));

        CaptchaResponse response = new CaptchaResponse();

        Captcha captcha = new SpecCaptcha(130, 48, 4);
        String verifyCode = captcha.text().toLowerCase();
        String key = UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
        redisTemplate.opsForValue().set(captchaType.getKeyPrefix() + key, verifyCode, 30, TimeUnit.MINUTES);

        response.setKey(key);
        response.setImage(captcha.toBase64());

        return Result.success(response);
    }

}