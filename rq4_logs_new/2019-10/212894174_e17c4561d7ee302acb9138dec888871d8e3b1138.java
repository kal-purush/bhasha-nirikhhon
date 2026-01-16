package com.project.controller;

import com.project.model.UserProfile;
import com.project.service.UserProfileServiceStub;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UserProfileControllerTest {

        private UserProfileController userProfileController;

        @Before
        public void initializeUserProfileController(){
            userProfileController = new UserProfileController();
            userProfileController.setUserProfileService(new UserProfileServiceStub());
        }

        @Test
        public void createUserProfile_saveUserProfile_Success() {
            UserProfile userProfile = new UserProfile();
            userProfile.setEmail("ben@gmail.com");
            userProfile.setMobile("913-874-3333");

            UserProfile newProfile = userProfileController.createUserProfile("ben", userProfile);

            Assert.assertNotNull(newProfile);
            Assert.assertEquals(newProfile.getEmail(), userProfile.getEmail());
            Assert.assertEquals(newProfile.getMobile(), userProfile.getMobile());
        }

    }
