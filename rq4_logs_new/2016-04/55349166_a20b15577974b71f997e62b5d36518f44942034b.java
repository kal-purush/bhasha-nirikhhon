package com.focuszz.mkfs.authenticator;

import java.util.Set;

import org.springframework.core.io.Resource;

import com.focuszz.mkfs.authenticator.definition.AuthenticationDefinition;
import com.focuszz.mkfs.authenticator.reader.AuthenticationDefinitionReader;
import com.focuszz.mkfs.authenticator.reader.XmlAuthenticationDefinitionReader;

public class AccessAuthenticator implements Authenticator {

    private Resource                       resource;

    private AuthenticationDefinitionReader definitionReader;

    private final String                   ROLE_ALL = XmlAuthenticationDefinitionReader.ROLE_ALL;

    public AccessAuthenticator() {

    }

    public AccessAuthenticator(Resource resource) {
        this(resource, new XmlAuthenticationDefinitionReader());
    }

    public AccessAuthenticator(Resource resource, AuthenticationDefinitionReader definitionReader) {
        if (null == resource) {
            throw new NullPointerException("resource can not be noll");
        }
        if (definitionReader == null) {
            throw new NullPointerException("definitionReader can not be noll");
        }
        this.resource = resource;
        this.definitionReader = definitionReader;
        definitionReader.readBeanDefinitions(resource);
    }

    public boolean authenticate(String uri, boolean login) {
        AuthenticationDefinition authDefinition = definitionReader.getRegistry().getBean(uri);
        if (authDefinition != null) {
            return authDefinition.isLogin() ? login : true;
        }
        return true;
    }

    public boolean authenticate(String uri, String role) {
        AuthenticationDefinition authDefinition = definitionReader.getRegistry().getBean(uri);
        Set<String> roles = null;
        if (authDefinition == null || (roles = authDefinition.getRoles()) == null) {
            return true;
        }
        return roles.contains(ROLE_ALL) || roles.contains(role);
    }

    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }

}