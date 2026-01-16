/**
 * This file is part of Expat
 * Copyright (C) 2023, Hopsworks AB. All rights reserved
 * <p>
 * Expat is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 * <p>
 * Expat is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <https://www.gnu.org/licenses/>.
 */

package io.hops.hopsworks.expat.migrations.onlinefs;

import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.featurestore.featureview.FeatureStoreMigration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class CreateOnlineFeaturestoreKafkaOffsetTable extends FeatureStoreMigration {
  private final static Logger LOGGER = LogManager.getLogger(CreateOnlineFeaturestoreKafkaOffsetTable.class);
  
  private final static String GET_ALL_PROJECTS = "SELECT projectname FROM `hopsworks`.`project`";
  private final static int GET_ALL_PROJECTS_S_PROJECTNAME = 1;

  private final static String CREATE_KAFKA_OFFSETS_TABLE =
          "CREATE TABLE IF NOT EXISTS `%s`.`kafka_offsets` (\n" +
          "`topic` varchar(255) COLLATE latin1_general_cs NOT NULL,\n" +
          "`partition` SMALLINT NOT NULL,\n" +
          "`offset` BIGINT UNSIGNED NOT NULL,\n" +
          "PRIMARY KEY (`topic`,`partition`)\n" +
          ") ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs";

  private final static String DROP_KAFKA_OFFSETS_TABLE =
          "DROP TABLE IF EXISTS `%s`.`kafka_offsets`";

  public void runMigration() throws MigrationException {
    // 1. Get all projects
    // 2. Create online featurestore kafka offset table
    try {
      connection.setAutoCommit(false);

      for (String projectName: getProjectNames()) {
        String sql = String.format(CREATE_KAFKA_OFFSETS_TABLE, projectName);
        if (!dryRun) {
          try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
          }
        }
      }
      connection.setAutoCommit(true);
      LOGGER.info("Online featurestore kafka offset table have been migrated.");
    } catch (SQLException e) {
      throw new MigrationException("Migration failed. Cannot commit.", e);
    } finally {
      super.close();
    }
  }

  public void runRollback() throws RollbackException {
    // 1. Get all projects
    // 2. Drop online featurestore kafka offset table
    try {
      connection.setAutoCommit(false);

      for (String projectName: getProjectNames()) {
        String sql = String.format(DROP_KAFKA_OFFSETS_TABLE, projectName);
        if (!dryRun) {
          try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
          }
        }
      }

      connection.setAutoCommit(true);
      LOGGER.info("Online featurestore kafka offset table have been rollback.");
    } catch (SQLException e) {
      throw new RollbackException("Rollback failed. Cannot commit.", e);
    } finally {
      super.close();
    }
  }

  private List<String> getProjectNames() throws SQLException {
    List<String> projectNames = new ArrayList<>();
    try (PreparedStatement statement = connection.prepareStatement(GET_ALL_PROJECTS)) {
      try (ResultSet resultSet = statement.executeQuery()) {
        projectNames.add(resultSet.getString(GET_ALL_PROJECTS_S_PROJECTNAME));
      }
    }
    return projectNames;
  }
}