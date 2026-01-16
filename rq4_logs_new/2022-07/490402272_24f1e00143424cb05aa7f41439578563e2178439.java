package com.trading.journal.authentication.configuration;

import com.trading.journal.authentication.ApplicationException;
import com.trading.journal.authentication.authority.Authority;
import com.trading.journal.authentication.authority.AuthorityCategory;
import com.trading.journal.authentication.authority.service.AuthorityService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class LoadAuthorities {

    private final AuthorityService authorityService;

    public Map<AuthorityCategory, String[]> getAuthorityCategoryMap() {
        Map<AuthorityCategory, String[]> categoryAuthorities = new ConcurrentHashMap<>();

        Arrays.stream(AuthorityCategory.values()).toList()
                .forEach(category -> {
                    String[] authorities;
                    List<Authority> authoritiesByCategory = authorityService.getAuthoritiesByCategory(category);
                    if (authoritiesByCategory == null || authoritiesByCategory.isEmpty()) {
                        throw new ApplicationException(HttpStatus.INTERNAL_SERVER_ERROR, "No authorities found in the database, please load it");
                    } else {
                        authorities = authoritiesByCategory.stream()
                                .map(Authority::getName)
                                .toArray(String[]::new);
                    }
                    categoryAuthorities.put(category, authorities);
                });
        return categoryAuthorities;
    }
}