/*
 * This file is part of the Meeds project (https://meeds.io/).
 *
 * Copyright (C) 2020 - 2025 Meeds Association contact@meeds.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 */
package io.meeds.chat.service;

import io.meeds.chat.model.Room;
import org.apache.commons.lang3.StringUtils;
import org.exoplatform.commons.utils.ListAccess;
import org.exoplatform.commons.utils.PropertyManager;
import org.exoplatform.container.ExoContainerContext;
import org.exoplatform.container.component.RequestLifeCycle;
import org.exoplatform.services.log.ExoLogger;
import org.exoplatform.services.log.Log;
import org.exoplatform.social.core.identity.SpaceMemberFilterListAccess;
import org.exoplatform.social.core.identity.model.Identity;
import org.exoplatform.social.core.identity.model.Profile;
import org.exoplatform.social.core.identity.provider.OrganizationIdentityProvider;
import org.exoplatform.social.core.manager.IdentityManager;
import org.exoplatform.social.core.profile.ProfileFilter;
import org.exoplatform.social.core.space.SpaceFilter;
import org.exoplatform.social.core.space.model.Space;
import org.exoplatform.social.core.space.spi.SpaceService;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import static io.meeds.chat.service.utils.MatrixConstants.*;

@Service
public class MatrixSynchronizationService {

  private static final Log    LOG                = ExoLogger.getExoLogger(MatrixSynchronizationService.class);

  private MatrixService       matrixService;

  private SpaceService        spaceService;

  private IdentityManager     identityManager;

  private int                 SPACES_THRESHOLD   = 20;

  private int                 LOADED_USERS_COUNT = 50;

  public MatrixSynchronizationService(MatrixService matrixService, SpaceService spaceService, IdentityManager identityManager) {
    this.matrixService = matrixService;
    this.spaceService = spaceService;
    this.identityManager = identityManager;
  }

  public void synchronizeSpaces() {
    long startupTime = System.currentTimeMillis();

    LOG.info("Start:: create Matrix rooms for spaces");
    int failedToMigrateSpaces = 0;
    int successfullyMigratedSpaces = 0;
    int spacesCount = 0;
    int ignoredSpaces = 0;

    RequestLifeCycle.begin(ExoContainerContext.getCurrentContainer());
    try {
      ListAccess<Space> spaces = spaceService.getAllSpacesByFilter(new SpaceFilter());
      spacesCount = spaces.getSize();
      int loadedSpaces = 0;
      while (loadedSpaces < spacesCount) {
        int actualSpacesToLoadCount =
                                    loadedSpaces + SPACES_THRESHOLD < spacesCount ? SPACES_THRESHOLD : spacesCount - loadedSpaces;
        Space[] spacesToMigrate = spaces.load(loadedSpaces, actualSpacesToLoadCount);
        for (Space space : spacesToMigrate) {
          Room room = matrixService.getRoomBySpace(space);
          if (room == null || StringUtils.isBlank(room.getRoomId())) {
            try {
              String roomId = this.matrixService.createRoom(space);
              for (String member : space.getMembers()) {
                Identity memberIdentity = identityManager.getOrCreateUserIdentity(member);
                if (memberIdentity != null
                    && StringUtils.isNotBlank((String) memberIdentity.getProfile().getProperty(USER_MATRIX_ID))) {
                  String matrixIdOfUser = (String) memberIdentity.getProfile().getProperty(USER_MATRIX_ID);
                  matrixService.joinUserToRoom(roomId, matrixIdOfUser);
                }
              }
              matrixService.updateRoomAvatar(space, roomId);
              successfullyMigratedSpaces++;
            } catch (Exception e) {
              LOG.error("Could not create a room for space {}", space.getDisplayName(), e);
              failedToMigrateSpaces++;
            }
          } else {
            matrixService.updateRoomAvatar(space, room.getRoomId());
            ignoredSpaces++;
            LOG.debug("The space {} has already a room with Id {}", space.getDisplayName(), room.getRoomId());
          }
        }
        loadedSpaces += spacesToMigrate.length;
      }
    } catch (Exception e) {
      throw new RuntimeException("Error while retrieving spaces", e);
    } finally {
      RequestLifeCycle.end();
    }
    LOG.info("Summary :: create Matrix rooms for spaces, {} created rooms for {} spaces, {} ignored spaces, {} rooms failed to be created !",
             successfullyMigratedSpaces,
             spacesCount,
             ignoredSpaces,
             failedToMigrateSpaces);
    if (failedToMigrateSpaces > 0) {
      throw new RuntimeException("Some spaces were not upgraded!");
    }
    LOG.info("End:: create Matrix rooms for spaces in {}", System.currentTimeMillis() - startupTime);
  }

  public void synchronizeUsers() {
    LOG.info("Start:: create Matrix accounts for users");
    long startupTime = System.currentTimeMillis();

    int checkedUsers = 0;
    int usersCount = 0;

    ListAccess<Identity> users = null;
    try {
      if (StringUtils.isNotBlank(PropertyManager.getProperty(MATRIX_RESTRICTED_USERS_GROUP))) {
        Space restrictedSpace = spaceService.getSpaceByGroupId(PropertyManager.getProperty(MATRIX_RESTRICTED_USERS_GROUP));
        if(restrictedSpace != null) {
          users = identityManager.getSpaceIdentityByProfileFilter(restrictedSpace, new ProfileFilter(), SpaceMemberFilterListAccess.Type.MEMBER, false);
        }
      } else {
        users = identityManager.getIdentitiesByProfileFilter(OrganizationIdentityProvider.NAME, new ProfileFilter(), false);
      }
      usersCount = users == null ? 0 : users.getSize();
    } catch (Exception e) {
      throw new RuntimeException("Error while checking users", e);
    }

    if (usersCount == 0) {
      throw new IllegalStateException("No users to migrate, please check the value of the property matrix.restricted.users.groupId or remove it to select all users.");
    }

    RequestLifeCycle.begin(ExoContainerContext.getCurrentContainer());
    try {
      while (checkedUsers < usersCount) {
        int usersToCheck = usersCount > checkedUsers + LOADED_USERS_COUNT ? LOADED_USERS_COUNT : (usersCount - checkedUsers);
        Identity[] usersArray = users.load(checkedUsers, usersToCheck);
        for (Identity user : usersArray) {
          Identity userIdentity = identityManager.getOrCreateUserIdentity(user.getRemoteId());
          Profile userProfile = userIdentity.getProfile();
          String userMatrixId = (String) userProfile.getProperty(USER_MATRIX_ID);
          String adminOfMatrix = PropertyManager.getProperty(MATRIX_ADMIN_USERNAME);
          if (StringUtils.isBlank(userMatrixId)) {
            try {
              boolean isNew = !user.getRemoteId().equals(adminOfMatrix);
              userMatrixId = matrixService.saveUserAccount(user, isNew);
            } catch (Exception e) {
              LOG.warn("Can not create the user {} on Matrix", user.getRemoteId(), e.getCause());
            }
          }
          if (StringUtils.isNotBlank(userMatrixId)) {
            // Update user avatar
            matrixService.updateUserAvatar(userProfile, userMatrixId);

            // Add user to spaces already sync with Matrix
            ListAccess<Space> userSpaces = spaceService.getMemberSpaces(user.getRemoteId());
            Space[] spaceArray = userSpaces.load(0, userSpaces.getSize());
            for (Space space : spaceArray) {
              Room room = matrixService.getRoomBySpace(space);
              if (room != null && StringUtils.isNotBlank(room.getRoomId())) {
                matrixService.joinUserToRoom(room.getRoomId(), userMatrixId);
              }
            }
          }
        }
        checkedUsers += usersArray.length;
        LOG.info("Checked Matrix account for {} of {} users", checkedUsers, usersCount);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error while creating accounts for users on Matrix", e);
    } finally {
      RequestLifeCycle.end();
    }
    LOG.info("Summary :: create Matrix accounts for {} users, {} users were checked with their Matrix accounts, {} accounts failed to be created !",
             checkedUsers,
             usersCount,
             usersCount - checkedUsers);
    if (usersCount - checkedUsers > 0) {
      throw new RuntimeException("Some user accounts were not synchronized with Matrix!");
    }
    LOG.info("End:: create Matrix accounts for users took {}", System.currentTimeMillis() - startupTime);
  }
}