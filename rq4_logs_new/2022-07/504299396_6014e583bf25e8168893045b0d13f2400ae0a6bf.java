package com.vk_media.vkmedia.aspect;

import com.vk_media.vkmedia.exception.VkUnauthenticatedException;
import com.vk_media.vkmedia.service.VkAuthService;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class VkAuthValidation {

    VkAuthService vkAuthService;

    public VkAuthValidation(VkAuthService vkAuthService) {
        this.vkAuthService = vkAuthService;
    }

    @Before("execution(* com.vk_media.vkmedia.controller.AlbumsController.*(..))")
    public void checkVkAuth() throws VkUnauthenticatedException {
        if (!vkAuthService.isAuthorized()) {
            throw new VkUnauthenticatedException();
        }
    }


}