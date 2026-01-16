package net.tch.java.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;

/**
 * @description:使用zookeeper原生api实现连接，新增，修改，删除的操作
 * @auth tongchenghao
 * @date 2020/2/18
 */
public class ZookeeperDemo {
    public static void main(String[] args) {
        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            // 连接zookeeper服务器
            ZooKeeper zooKeeper = new ZooKeeper("192.168.137.5:2181", 4000, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    // 当连接状态为Event.KeeperState.SyncConnected时表示连接真正建立
                    if(watchedEvent.getState().equals(Event.KeeperState.SyncConnected)){
                        // 此时停止阻塞线程
                        countDownLatch.countDown();
                    }
                }
            });
            System.out.println(zooKeeper.getState());// 此时连接状态为CONNECTING
            System.out.println("连接开始时间:"+new Date().getTime());// 连接开始时间
            // 只有当连接状态为CONNECTED时zookeeper才能正常使用,所以一下操作应该等到连接真正建立后进行
            // 此时我们可以阻塞线程，等待监听确定连接已经真正建立再运行
            countDownLatch.await();
            System.out.println("连接结束时间:"+new Date().getTime());// 连接结束时间
            System.out.println(zooKeeper.getState());// 此时连接状态为CONNECTED
            /**
             * 创建一个节点
             * create方法参数说明：
             *      path:节点地址
             *      data:节点的value值
             *      acl：节点的权限（ZooDefs.Ids.OPEN_ACL_UNSAFE：公开节点）
             *      createMode：节点的特性（持久化节点，持久化有序节点，临时节点，临时有序节点等）
             */
            zooKeeper.create("/test","0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            Stat stat = new Stat();
            System.out.println("开始获取节点");
            byte[] oldDate = zooKeeper.getData("/test", null, stat);
            System.out.println(new String(oldDate));
            System.out.println("开始修改节点");
            zooKeeper.setData("/test","1".getBytes(),stat.getVersion());
            byte[] newDate = zooKeeper.getData("/test", null, stat);
            System.out.println(new String(newDate));
            System.out.println("开始删除节点");
            zooKeeper.delete("/test",stat.getVersion());
            System.out.println("完成删除节点");
            zooKeeper.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}