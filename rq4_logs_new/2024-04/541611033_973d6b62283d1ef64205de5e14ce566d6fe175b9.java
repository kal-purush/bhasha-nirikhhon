package org.aibles.user_profile.configuration;

import org.aibles.user_profile.facade.ImageFacadeService;
import org.aibles.user_profile.facade.PostFacadeService;
import org.aibles.user_profile.facade.UserProfileFacadeService;
import org.aibles.user_profile.facade.impl.ImageFacadeServiceImpl;
import org.aibles.user_profile.facade.impl.PostFacadeServiceImpl;
import org.aibles.user_profile.facade.impl.UserProfileFacadeServiceImpl;
import org.aibles.user_profile.repository.ImageRepository;
import org.aibles.user_profile.repository.PostRepository;
import org.aibles.user_profile.repository.UserProfileRepository;
import org.aibles.user_profile.service.ImageService;
import org.aibles.user_profile.service.PostService;
import org.aibles.user_profile.service.UserProfileService;
import org.aibles.user_profile.service.impl.ImageServiceImpl;
import org.aibles.user_profile.service.impl.PostServiceImpl;
import org.aibles.user_profile.service.impl.UserProfileServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EnableJpaRepositories(basePackages = {"org.aibles.user_profile.repository"})
@ComponentScan(basePackages = {"org.aibles.user_profile.repository"})
@EnableJpaAuditing
public class UserProfileConfiguration {

  @Bean
  public UserProfileService userProfileService(UserProfileRepository repository) {
    return new UserProfileServiceImpl(repository);
  }

  @Bean
  public PostService postService(PostRepository repository) {
    return new PostServiceImpl(repository);
  }

  @Bean
  public ImageService imageService(ImageRepository repository) {
    return new ImageServiceImpl(repository);
  }

  @Bean
  public PostFacadeService postFacadeService(UserProfileService userProfileService, PostService postService, ImageService imageService) {
    return new PostFacadeServiceImpl(userProfileService, postService, imageService);
  }

  @Bean
  public ImageFacadeService imageFacadeService(UserProfileService userProfileService, PostService postService, ImageService imageService) {
    return new ImageFacadeServiceImpl(userProfileService, postService, imageService);
  }

  @Bean
  public UserProfileFacadeService userProfileFacadeService(UserProfileService userProfileService, PostFacadeService postFacadeService) {
    return new UserProfileFacadeServiceImpl(userProfileService, postFacadeService);
  }
}