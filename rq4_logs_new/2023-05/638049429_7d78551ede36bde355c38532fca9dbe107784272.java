package com.auth.auth.configs;

import com.auth.auth.repos.SiteUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

@Service
public interface UserDetailServiceImpl implements UserDetailsService {
    @Autowired
    SiteUserRepository siteUserRepository;

    @Override
    public UserDetails loadUserByUserName(String username) throws UsernameNotFoundException {
        return siteUserRepository;
    }
}