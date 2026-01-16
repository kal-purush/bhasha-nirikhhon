/**
 * Copyright (c) 2012-2015 Nord Trading Network.
 *
 * http://www.nordpos.mobi
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package mobi.nordpos.dao.factory;

import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.support.ConnectionSource;
import java.sql.SQLException;
import java.util.List;
import mobi.nordpos.dao.model.Location;
import mobi.nordpos.dao.ormlite.LocationDao;

/**
 * @author Andrey Svininykh <svininykh@gmail.com>
 */
public class LocationPersist implements PersistFactory {

    ConnectionSource connectionSource;
    LocationDao locationDao;

    @Override
    public void init(ConnectionSource connectionSource) throws SQLException {
        this.connectionSource = connectionSource;
        locationDao = new LocationDao(connectionSource);
    }

    @Override
    public Location read(Object id) throws SQLException {
        try {
            return locationDao.queryForId((String) id);
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

    @Override
    public List<Location> readList() throws SQLException {
        try {
            QueryBuilder qb = locationDao.queryBuilder().orderBy(Location.NAME, true);
            qb.where().isNotNull(Location.ID);
            return qb.query();
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

    @Override
    public Location find(String column, Object value) throws SQLException {
        try {
            QueryBuilder qb = locationDao.queryBuilder();
            qb.where().like(column, value);
            return (Location) qb.queryForFirst();
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

    @Override
    public Location add(Object location) throws SQLException {
        try {
            return locationDao.createIfNotExists((Location) location);
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

    @Override
    public Boolean change(Object location) throws SQLException {
        try {
            return locationDao.update((Location) location) > 0;
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

    @Override
    public Boolean delete(Object id) throws SQLException {
        try {
            return locationDao.deleteById((String) id) > 0;
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }
}