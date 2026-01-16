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

import com.j256.ormlite.stmt.PreparedUpdate;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.stmt.UpdateBuilder;
import com.j256.ormlite.stmt.mapped.MappedPreparedStmt;
import com.j256.ormlite.support.ConnectionSource;
import java.sql.SQLException;
import java.util.List;
import mobi.nordpos.dao.model.Location;
import mobi.nordpos.dao.model.Product;
import mobi.nordpos.dao.model.StockCurrent;
import mobi.nordpos.dao.ormlite.StockCurrentDao;

/**
 * @author Andrey Svininykh <svininykh@gmail.com>
 */
public class StockCurrentPersist implements PersistFactory {

    ConnectionSource connectionSource;
    StockCurrentDao stockCurrentDao;

    @Override
    public void init(ConnectionSource connectionSource) throws SQLException {
        this.connectionSource = connectionSource;
        stockCurrentDao = new StockCurrentDao(connectionSource);
    }

    @Override
    public StockCurrent read(Object id) throws SQLException {
        try {
            return stockCurrentDao.queryForId((String) id);
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

    public StockCurrent read(Location location, Product product) throws SQLException {
        try {
            QueryBuilder qb = stockCurrentDao.queryBuilder();
            qb.where().like(StockCurrent.LOCATION, location).and().like(StockCurrent.PRODUCT, product);
            return (StockCurrent) qb.queryForFirst();
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }
    
    @Override
    public List<StockCurrent> readList() throws SQLException {
        try {
            return stockCurrentDao.queryForAll();
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

    @Override
    public StockCurrent find(String column, Object value) throws SQLException {
        try {
            QueryBuilder qb = stockCurrentDao.queryBuilder();
            qb.where().like(column, value);
            return (StockCurrent) qb.queryForFirst();
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

    @Override
    public StockCurrent add(Object stockCurrent) throws SQLException {
        try {
            return stockCurrentDao.createIfNotExists((StockCurrent) stockCurrent);
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

    @Override
    public Boolean change(Object stockCurrent) throws SQLException {
        try {
            StockCurrent stock = (StockCurrent) stockCurrent;
            UpdateBuilder<StockCurrent, String> ub = stockCurrentDao.updateBuilder();
            ub.where().like(StockCurrent.LOCATION, stock.getLocation()).and().like(StockCurrent.PRODUCT, stock.getProduct());
            ub.updateColumnValue(StockCurrent.UNITS, stock.getUnit());            
            return ub.update() > 0;
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

    @Override
    public Boolean delete(Object id) throws SQLException {
        try {
            return stockCurrentDao.deleteById((String) id) > 0;
        } finally {
            if (connectionSource != null) {
                connectionSource.close();
            }
        }
    }

}