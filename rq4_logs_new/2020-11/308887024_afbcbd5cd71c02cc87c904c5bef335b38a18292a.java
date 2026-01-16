package com.marketplace;

import com.marketplace.config.ApplicationConfig;
import com.marketplace.config.ConfigLoader;
import com.marketplace.context.ApplicationContext;
import com.marketplace.context.DaggerApplicationContext;
import com.marketplace.context.mongo.MongoConfig;
import com.marketplace.context.mongo.MongoConfigModule;
import com.marketplace.controller.userprofile.CreateUserProfileDto;
import com.marketplace.controller.userprofile.UpdateUserProfileDto;
import com.marketplace.domain.shared.UserId;
import com.marketplace.domain.userprofile.DisplayName;
import com.marketplace.domain.userprofile.UserProfile;
import com.marketplace.domain.userprofile.entity.UserProfileEntity;
import com.marketplace.domain.userprofile.repository.UserProfileRepository;
import com.marketplace.fixtures.UserProfileFixture;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class UserProfileRepositoryTest extends AbstractContainerInitializer {

    String insertId = "0b8a557d-32f6-4268-80d5-6a38df8a9520";
    MongoConfig mongoConfig;
    MongoClient mongoClient;
    MongoCollection<UserProfileEntity> classifiedAdCollection;
    ApplicationContext context;
    UserProfileRepository userProfileRepository;

    @BeforeEach
    void setup() throws IOException {
        ApplicationConfig config = ConfigLoader.loadClasspathResource("application.yml", ApplicationConfig.class);

        context = DaggerApplicationContext.
                builder()
                .config(config)
                .build();

        String hosts = mongoDBContainer.getHost();
        int port = mongoDBContainer.getMappedPort(27017);
        mongoConfig = new MongoConfig(hosts, "test_db", port);
        config.setMongoConfig(mongoConfig);
        mongoClient = MongoConfigModule.provideMongoClient(mongoConfig);
        userProfileRepository = context.getUserProfileRepository();
    }

    @AfterEach
    public void cleanup() {
        userProfileRepository.deleteAll();
    }

    @Test
    void userProfileCanBeCreated() throws IOException {
        CreateUserProfileDto createUserProfileDto = UserProfileFixture.loadCreateUserProfileDto();
        assertThat(createUserProfileDto).isNotNull();
        assertThat(createUserProfileDto.getFirstName()).isNotBlank();
        assertThat(createUserProfileDto.getMiddleName()).isNotBlank();
        assertThat(createUserProfileDto.getLastName()).isNotBlank();

        UserProfile userProfile = new UserProfile(UserId.fromString(insertId), createUserProfileDto.getFullName(),
                new DisplayName(createUserProfileDto.getDisplayName()));

        UserProfile add = userProfileRepository.add(userProfile);

        assertThat(add).isNotNull();
    }

    @Test
    void userProfileCanBeCreatedAndLoaded() throws IOException {
        CreateUserProfileDto createUserProfileDto = UserProfileFixture.loadCreateUserProfileDto();
        assertThat(createUserProfileDto).isNotNull();
        assertThat(createUserProfileDto.getFirstName()).isNotBlank();
        assertThat(createUserProfileDto.getMiddleName()).isNotBlank();
        assertThat(createUserProfileDto.getLastName()).isNotBlank();

        UserProfile userProfile = new UserProfile(UserId.fromString(insertId), createUserProfileDto.getFullName(),
                new DisplayName(createUserProfileDto.getDisplayName()));

        UserProfile add = userProfileRepository.add(userProfile);

        assertThat(add).isNotNull();

        Optional<UserProfile> load = userProfileRepository.load(add.getId());
        assertThat(load).isPresent();

        UserProfile savedUserProfile = load.get();

        assertThat(savedUserProfile.getId()).isNotNull();
        assertThat(savedUserProfile.getChanges()).hasSize(1);
    }

    @Test
    void userProfileCanBeUpdated() throws IOException {
        CreateUserProfileDto createUserProfileDto = UserProfileFixture.loadCreateUserProfileDto();
        UpdateUserProfileDto updateUserProfileDto = UserProfileFixture.loadUpdateUserProfileDto();
        assertThat(createUserProfileDto).isNotNull();
        assertThat(createUserProfileDto.getFirstName()).isNotBlank();
        assertThat(createUserProfileDto.getMiddleName()).isNotBlank();
        assertThat(createUserProfileDto.getLastName()).isNotBlank();

        UserProfile userProfile = new UserProfile(UserId.fromString(insertId), createUserProfileDto.getFullName(),
                new DisplayName(createUserProfileDto.getDisplayName()));

        UserProfile add = userProfileRepository.add(userProfile);

        assertThat(add).isNotNull();

        add.updatePhoto(updateUserProfileDto.getPhotoUrl());
        UserProfile secondSaved = userProfileRepository.add(add);
        assertThat(secondSaved).isNotNull();

        Optional<UserProfile> load = userProfileRepository.load(add.getId());
        assertThat(load).isPresent();

        UserProfile savedUserProfile = load.get();

        assertThat(savedUserProfile.getId()).isNotNull();
        assertThat(savedUserProfile.getChanges()).hasSize(2);
        assertThat(savedUserProfile.getPhotoUrl()).isEqualTo("http://photos.me/user-2.jpg");
    }

    @Test
    void userProfileCanBeCreatedAndShownToExist() throws IOException {
        CreateUserProfileDto createUserProfileDto = UserProfileFixture.loadCreateUserProfileDto();
        assertThat(createUserProfileDto).isNotNull();
        assertThat(createUserProfileDto.getFirstName()).isNotBlank();
        assertThat(createUserProfileDto.getMiddleName()).isNotBlank();
        assertThat(createUserProfileDto.getLastName()).isNotBlank();

        UserProfile userProfile = new UserProfile(UserId.fromString(insertId), createUserProfileDto.getFullName(),
                new DisplayName(createUserProfileDto.getDisplayName()));

        UserProfile add = userProfileRepository.add(userProfile);

        assertThat(add).isNotNull();

        boolean exists = userProfileRepository.exists(add.getId());
        assertThat(exists).isTrue();
    }

}