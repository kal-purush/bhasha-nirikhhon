package com.github.lukaszbudnik.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.TransactionSetDataBuilder;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;

class TestPathChildrenCacheListener implements PathChildrenCacheListener {

    private byte[] newValue;

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
            newValue = event.getData().getData();
        }
    }

    public byte[] getNewValue() {
        return newValue;
    }
}

public class CuratorTest {

    private static CuratorFramework curatorFramework1;

    private static CuratorFramework curatorFramework2;

    private static CuratorFramework curatorFrameworkUnAuthorized;

    @BeforeClass
    public static void setUp() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        EnsembleProvider ensembleProvider = new FixedEnsembleProvider("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory
                .builder()
                .ensembleProvider(ensembleProvider)
                .retryPolicy(retryPolicy);

        curatorFrameworkUnAuthorized = builder.build();
        curatorFrameworkUnAuthorized.start();

        CuratorFrameworkFactory.Builder builderWithAuthorization = builder
                .authorization("digest", "lukaszbudnik:supersecret".getBytes());

        curatorFramework1 = builderWithAuthorization.build();
        curatorFramework1.start();

        curatorFramework2 = builderWithAuthorization.build();
        curatorFramework2.start();
    }

    @AfterClass
    public static void tearDown() {
        curatorFramework1.close();
        curatorFramework2.close();
        curatorFrameworkUnAuthorized.close();
    }

    @Test
    public void zoo() throws Exception {

        final String path = "/dev/database/username";
        final byte[] value1 = "user".getBytes();
        final byte[] value2 = "newuser".getBytes();

        // 1. client 1 will create a new znode
        createZNode(curatorFramework1, path, value1);

        // 2. client 1 verifies if the znode exists
        existsZNode(curatorFramework1, path);

        // 3. client 1 adds a listener for children of "/dev/database"
        TestPathChildrenCacheListener pathChildrenCacheListener = createAndAddPathChildrenCacheListener(curatorFramework1);

        // 4. client 2 updates the znode in transaction
        updateZNodeInTransaction(curatorFramework2, path, value2);

        // 5 client 1 cache listener should get an async event by now, just in case wait for a second (or two...)
        Thread.sleep(2 * 1000);
        Assert.assertArrayEquals(value2, pathChildrenCacheListener.getNewValue());

        // 5 client 1 gets the data the sync way
        getDataZNode(curatorFramework1, path, value2);

        // 6. Unauthorized client tries to get the data
        try {
            getDataZNode(curatorFrameworkUnAuthorized, path, value2);
            Assert.fail("Should throw KeeperException.NoAuthException");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KeeperException.NoAuthException);
        }

        // 7. client 1 deletes znode
        deleteZNode(curatorFramework1, path);

        // 8. client 2 verifies if znode does not exist
        notExistsZNode(curatorFramework2, path);
    }

    private void notExistsZNode(CuratorFramework curatorFramework, String path) throws Exception {
        ExistsBuilder existsBuilder = curatorFramework.checkExists();

        Stat exists = existsBuilder.forPath(path);

        Assert.assertNull(exists);
    }

    private void deleteZNode(CuratorFramework curatorFramework, String path) throws Exception {
        DeleteBuilder deleteBuilder = curatorFramework.delete();
        deleteBuilder.forPath(path);
    }

    private void getDataZNode(CuratorFramework curatorFramework, String path, byte[] value2) throws Exception {
        GetDataBuilder getDataBuilder = curatorFramework.getData();
        byte[] read = getDataBuilder.forPath(path);
        Assert.assertArrayEquals(value2, read);
    }

    private TestPathChildrenCacheListener createAndAddPathChildrenCacheListener(CuratorFramework curatorFramework) throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, "/dev/database", true);
        TestPathChildrenCacheListener pathChildrenCacheListener = new TestPathChildrenCacheListener();
        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
        pathChildrenCache.start();
        return pathChildrenCacheListener;
    }

    private void updateZNodeInTransaction(CuratorFramework curatorFramework, String path, byte[] value) throws Exception {
        CuratorTransaction transaction = curatorFramework.inTransaction();
        TransactionSetDataBuilder transactionSetDataBuilder = transaction.setData();

        CuratorTransactionBridge transactionBridge = transactionSetDataBuilder.forPath(path, value);

        Collection<CuratorTransactionResult> transactionResults = transactionBridge.and().commit();

        CuratorTransactionResult transactionResult = transactionResults.iterator().next();

        Stat updated = transactionResult.getResultStat();

        Assert.assertNotNull(updated);
        Assert.assertTrue(updated.getVersion() == 1);
        Assert.assertTrue(updated.getMtime() > updated.getCtime());
    }

    private void existsZNode(CuratorFramework curatorFramework, String path) throws Exception {
        ExistsBuilder existsBuilder = curatorFramework.checkExists();

        Stat exists = existsBuilder.forPath(path);

        Assert.assertNotNull(exists);
        Assert.assertTrue(exists.getVersion() == 0);
        Assert.assertTrue(exists.getCtime() == exists.getMtime());
    }

    private void createZNode(CuratorFramework curatorFramework, String path, byte[] value) throws Exception {
        CreateBuilder createBuilder = curatorFramework.create();
        createBuilder.creatingParentsIfNeeded();

        String result = createBuilder.withACL(ZooDefs.Ids.CREATOR_ALL_ACL).forPath(path, value);

        Assert.assertEquals(path, result);
    }

}