package ZOOKEEPER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;

public class DistributedLock implements Lock, Watcher {

    private ZooKeeper zk;
    private String root = "/locks";//根
    private String lockName;//竞争资源的标志
    private String waitNode;//等待前一个锁
    private String myZnode;//当前锁
    private CountDownLatch latch;//计数器
    private CountDownLatch connectedSingle = new CountDownLatch(1);
    private int sessionTimeOut;
    private Properties properties;

    DistributedLock() {

    }
    /**
     * 创建分布式锁,使用前请确认config配置的zookeeper服务可用
     */
    public DistributedLock(Properties properties, String lockName) {
        this.lockName = lockName;
        // 创建一个与服务器的连接
        try {
            String host = properties.getProperty("ZK_HOST");
            String port = properties.getProperty("ZK_PORT");
            int sessionTimeOut = Integer.parseInt(properties.getProperty("sessionTimeOut"));
            this.sessionTimeOut = sessionTimeOut;

            String url = host + ":" + port;
            zk = new ZooKeeper(url, sessionTimeOut, this);//此去不执行 Watcher
            connectedSingle.await();
            Stat stat = zk.exists(root, false);
            if (stat == null) {
                // 创建根节点
                zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception ioe) {
            throw new LockException(ioe);
        }
    }

    /**
     * zookeeper节点的监视器
     */
    public void process(WatchedEvent watchedEvent) {
        //建立连接用
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            connectedSingle.countDown();
            return;
        }
        //其他线程放弃锁的标志
        if (this.latch != null) {
            this.latch.countDown();
        }
    }

    public void lock() {
        try {
            if (this.tryLock()) {
                System.out.println("Thread " + Thread.currentThread().getId() + myZnode + " get lock true");
                return;
            } else {
                waitForLock(waitNode, sessionTimeOut);
            }
        } catch (Exception e) {
            throw new LockException(e);
        }
    }

    private boolean waitForLock(String waitNode, long sessionTimeOut) throws KeeperException, InterruptedException {
        //判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听
        Stat stat = zk.exists(root + "/" + waitNode, true);
        if (stat != null) {
            System.out.println("Thread " + Thread.currentThread().getId() + " waiting for " + root + "/" + waitNode);
            this.latch = new CountDownLatch(1);
            this.latch.await(sessionTimeOut, TimeUnit.MILLISECONDS);
            this.latch = null;
        }
        return true;
    }

    public void lockInterruptibly() throws InterruptedException {
        this.lock();

    }

    public boolean tryLock() {
        try {
            String splitStr = "_lock_";
            if (lockName.contains(splitStr)) {
                throw new LockException("lockName can not contains \\u000B");
            }

            myZnode = zk.create(root + "/" + lockName + splitStr, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(myZnode + " is Created");

            List<String> nodes = zk.getChildren(root, false);
            List<String> lockObjNodes = new ArrayList<String>();
            for (String node : nodes) {
                String _node = node.split(splitStr)[0];
                if (_node.equalsIgnoreCase(lockName)) {
                    lockObjNodes.add(node);
                }
            }
            Collections.sort(lockObjNodes);

            if (myZnode.equals(root+"/"+lockObjNodes.get(0))) {
                System.out.println(myZnode + "==" + lockObjNodes.get(0));
                return true;
            }

            String subMyNode = myZnode.substring(myZnode.lastIndexOf("/")+1);
            waitNode = lockObjNodes.get(Collections.binarySearch(lockObjNodes, subMyNode) - 1);
        } catch (Exception ex) {
            throw new LockException(ex);
        }
        return false;
    }

    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        try {
            if (this.tryLock()) {
                return true;
            }
            return waitForLock(waitNode, time);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public void unlock() {
        try {
            System.out.println("unlock" + myZnode);
            zk.delete(myZnode, -1);
            myZnode = null;
            zk.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public Condition newCondition() {
        return null;
    }


    public class LockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public LockException(String e){
            super(e);
        }
        public LockException(Exception e){
            super(e);
        }
    }
//
//    public void setProperties(Properties properties) {
//        this.properties = properties;
//    }
//
//    public Properties getProperties() {
//        return properties;
//    }
}