package com.yz.community.community.HelloController;

import com.yz.community.community.Provider.GitHubProvider;
import com.yz.community.community.dto.AccessTokenDto;
import com.yz.community.community.dto.GitHubUsedrDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.info.ProjectInfoProperties;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * create by yuanze
 */
@Controller
public class AuthorizeController {
    @Autowired
    private GitHubProvider gitHubProvider;

    @GetMapping("/callback")
    public String callback(@RequestParam(name = "code") String code,
                           @RequestParam(name = "state") String state){
        AccessTokenDto accessTokenDto = new AccessTokenDto();
        accessTokenDto.setCode(code);
        accessTokenDto.setState(state);
        accessTokenDto.setRedirect_uri("http://localhost:8887/callback");
        accessTokenDto.setClient_id("6914b8c651c8ebf784de");
        accessTokenDto.setClient_secret("dad3af38a161f2fc315013f21ca0e89824910515");
        String accessToken = gitHubProvider.getAccessToken(accessTokenDto);
        GitHubUsedrDto user = gitHubProvider.getUser(accessToken);
        System.out.println(user.getName());
        return "index";
    }
}