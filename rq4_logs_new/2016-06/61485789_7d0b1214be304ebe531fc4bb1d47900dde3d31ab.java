package com.github.scribejava.apis;

import com.github.scribejava.apis.service.SkypeOAuthServiceImpl;
import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.model.OAuthConfig;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

public class SkypeApi extends DefaultApi20 {

    public SkypeApi() {
    }

    public static SkypeApi instance() {
        return InstanceHolder.INSTANCE;
    }

    @Override
    public Verb getAccessTokenVerb() {
        return Verb.POST;
    }

    @Override
    public String getAccessTokenEndpoint() {
        return "https://login.microsoftonline.com/common/oauth2/v2.0/token";
    }

    @Override
    protected String getAuthorizationBaseUrl() {
        return "https://login.microsoftonline.com/common/oauth2/v2.0/authorize";
    }

    @Override
    public OAuth20Service createService(OAuthConfig config) {
        return new SkypeOAuthServiceImpl(this, config);
    }

    private static class InstanceHolder {
        private static final SkypeApi INSTANCE = new SkypeApi();
    }
}