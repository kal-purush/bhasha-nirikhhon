package org.example.module.common.captcha.service.impl;

import com.google.common.base.Preconditions;
import com.wf.captcha.SpecCaptcha;
import com.wf.captcha.base.Captcha;
import org.example.common.constant.MsgKeyConstant;
import org.example.common.util.ConvertUtil;
import org.example.common.util.MessageUtil;
import org.example.module.common.captcha.enums.CaptchaType;
import org.example.module.common.captcha.service.ICaptchaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

/**
 * @author linzhaoming
 * @since 2020-12-18
 **/
@Service
public class CaptchaServiceImpl implements ICaptchaService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public String generateCaptcha(@NotNull Integer type, @NotBlank String uuid) {
        CaptchaType captchaType = CaptchaType.get(type);
        Preconditions.checkNotNull(captchaType, MessageUtil.get(MsgKeyConstant.CAPTCHA_TYPE_NOT_EXISTED));

        String key = captchaType.getKeyPrefix() + uuid;

        if (ConvertUtil.toBooleanValue(redisTemplate.hasKey(key), false)) {
            redisTemplate.delete(key);
        }

        Captcha captcha = new SpecCaptcha(130, 48, 4);
        String verifyCode = captcha.text().toLowerCase();

        redisTemplate.opsForValue().set(key, verifyCode, 30, TimeUnit.MINUTES);

        return captcha.toBase64();
    }
}