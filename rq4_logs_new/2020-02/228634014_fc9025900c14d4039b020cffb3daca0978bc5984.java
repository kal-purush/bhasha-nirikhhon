package net.tch.java.zookeeper.distributedLock;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @description:基于zookeeper实现分布式锁
 * @auth tongchenghao
 * @date 2020/2/17
 */
public class DistributeLock implements Lock, Watcher {

    private ZooKeeper zk = null;// 定义一个zookeeper的连接
    private String ROOT_LOCK = "/locks";// 定义根节点
    private String CURR_LOCK;// 定义当前的锁
    private String WAIT_LOCK;// 当前等待中的锁

    private CountDownLatch countDownLatch;

    public DistributeLock() {
        try {
            zk = new ZooKeeper("192.168.137.5:2181",4000,this);
            // 判断根节点是否存在
            Stat stat = zk.exists(ROOT_LOCK,false);
            if(stat==null){
                // 如果不存在则创建持久化的根节点
                zk.create(ROOT_LOCK,"0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lock() {
        if(this.tryLock()){
            System.out.println(Thread.currentThread().getName()+"->"+CURR_LOCK+"获得锁成功");
            return;
        }
        try {
            // 等待获得锁
            waitForLock(WAIT_LOCK);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 等待前一个节点释放锁，并自身获得锁
     * @param prev
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void waitForLock(String prev) throws KeeperException, InterruptedException {
        Stat prevNode = zk.exists(prev, true);
        if (prevNode!=null){
            System.out.println(Thread.currentThread().getName()+"->等待锁"+prev+"释放锁");
            // 获得锁失败则阻塞线程
            countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
            // 线程阻塞结束则获得锁
            System.out.println(Thread.currentThread().getName()+"->"+CURR_LOCK+"获得锁");
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        // 尝试获取锁的时候直接先去创建一个临时有序节点
        try {
            CURR_LOCK = zk.create(ROOT_LOCK+"/","0".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName()+"->"+CURR_LOCK+"尝试竞争锁");
            // 获得根节点下所有的节点
            List<String> childrens = zk.getChildren(ROOT_LOCK, false);
            // 通过SortedSet对所有节点进行排序
            SortedSet<String> sortedSet = new TreeSet<>();
            for (String children:childrens){
                sortedSet.add(ROOT_LOCK+"/"+children);
            }
            // 获取第一个值
            String firstNode = sortedSet.first();
            if(firstNode.equals(CURR_LOCK)){
                // 如果当前值就是最小的则获得锁
                return true;
            }
            // 获取比当前锁更小的集合
            SortedSet<String> lessThenMe = sortedSet.headSet(CURR_LOCK);
            if(!lessThenMe.isEmpty()){
                // 如果存在比当前锁更小的值，则把更小值集合中最大的作为等待锁
                WAIT_LOCK = lessThenMe.last();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        System.out.println(Thread.currentThread().getName()+"->释放锁"+CURR_LOCK);
        try {
            zk.delete(CURR_LOCK,-1);
            CURR_LOCK=null;
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        // 在前面的代码中，为WAIT_LOCK节点配置了监听事件，节点发生删除时，zookeeper会调用监听事件，进入事件后，关闭阻塞
        if(countDownLatch!=null){
            countDownLatch.countDown();
        }
    }
}