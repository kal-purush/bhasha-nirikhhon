package io.dropwizard.sharding.bundle;

import io.dropwizard.sharding.DBShardingBundle;
import io.dropwizard.sharding.config.ShardedHibernateFactory;

/**
 * Created by tushar.mandar on 4/25/17.
 */
public class DBShardingBundleWithAnnotationTest extends DBShardingBundleBaseTest {

    @Override
    protected DBShardingBundle<DBShardingBundleBaseTest.TestConfig> getBundle() {
        return new DBShardingBundle<DBShardingBundleBaseTest.TestConfig>("io.dropwizard.sharding.dao.testdata.entities") {
            @Override
            protected ShardedHibernateFactory getConfig(DBShardingBundleBaseTest.TestConfig config) {
                return testConfig.getShards();
            }
        };
    }
}