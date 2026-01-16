/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing.postgresql;

import com.eventsourcing.Repository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.Setter;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

class PooledDataSource {

    private static Map<Repository, PooledDataSource> instance = new HashMap<>();

    public static PooledDataSource getInstance(Repository repository) {
        return instance.computeIfAbsent(repository, (r) -> new PooledDataSource());
    }

    @Getter @Setter
    private DataSource dataSource;

    @Getter @Setter
    private HikariConfig hikariConfig = new HikariConfig();

    public DataSource getDataSource() {
        if (dataSource == null) {
            dataSource = new HikariDataSource(hikariConfig);
        }
        return dataSource;
    }

}