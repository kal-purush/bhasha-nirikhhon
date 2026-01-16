package com.github.britter.springbootherokudemo.repo_mod;

import com.github.britter.springbootherokudemo.model.security.Authority;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AuthorityRepo extends JpaRepository<Authority,String> {
    Authority findByName(String name);
}