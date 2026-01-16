package com.directori.book.springboot.web;

import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
public class ProfileController {

  private final Environment env;

  @GetMapping("/profile")
  public String profile() {
    // 현재 활성화된 profiles를 모두 가져온다. real, oauth, real-db
    List<String> profiles = Arrays.asList(env.getActiveProfiles());
    // 무중단 배포에서는 real1, real2만 사용됨
    List<String> realProfiles = Arrays.asList("real", "real1", "real2");
    String defaultProfile = profiles.isEmpty() ? "default" : profiles.get(0);
    return profiles.stream()
        .filter(realProfiles::contains)
        .findAny()
        .orElse(defaultProfile);
  }
}