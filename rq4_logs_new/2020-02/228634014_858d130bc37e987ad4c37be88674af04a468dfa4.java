package net.tch.java.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * @description:curator客户端简单使用
 * @auth tongchenghao
 * @date 2020/2/19
 */
public class CuratorDemo {
    public static void main(String[] args) {

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        String nodePath = "/test";

        // 创建客户端连接
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString("192.168.137.5:2181")
                .connectionTimeoutMs(1000)
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                .namespace("curator")
                .build();
        curatorFramework.getConnectionStateListenable().addListener((curatorFramework1, connectionState) -> {
            if(connectionState.isConnected()){
                System.out.println(new Date().getTime());
                countDownLatch.countDown();
            }
        });
        // 启动客户端
        curatorFramework.start();
        System.out.println(new Date().getTime());
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            // 创建节点
            String node = curatorFramework.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(nodePath,"0".getBytes());
            System.out.println(node+"节点创建成功");
            Thread.sleep(1000);
            System.out.println("--------------------------");
            System.out.println("开始注册事件");
            //添加事件
            addNodeCache(curatorFramework,nodePath);
            addPathChildCache(curatorFramework,nodePath);
            addTreeCache(curatorFramework,nodePath);
            System.out.println("事件注册完成");
            Thread.sleep(1000);
            System.out.println("--------------------------");
            Stat stat = new Stat();
            // 获取节点值
            String value = new String(curatorFramework.getData().storingStatIn(stat).forPath(nodePath));
            System.out.println("修改前值为"+value);
            Thread.sleep(1000);
            System.out.println("--------------------------");
            // 修改节点
            System.out.println("开始修改test节点");
            stat = curatorFramework.setData().withVersion(stat.getVersion()).forPath(nodePath,"xx".getBytes());
            // 获取节点值
            value = new String(curatorFramework.getData().storingStatIn(stat).forPath(nodePath));
            System.out.println("修改后值为"+value);
            Thread.sleep(1000);
            System.out.println("--------------------------");
            // 添加子节点
            System.out.println("开始添加node1子节点");
            curatorFramework.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(nodePath+"/node1","0".getBytes());
            System.out.println("添加成功");
            Thread.sleep(1000);
            System.out.println("--------------------------");
            // 修改节点
            System.out.println("开始修改node1子节点");
            Stat stat2 = curatorFramework.setData().withVersion(stat.getVersion()).forPath(nodePath, "xx2".getBytes());
            System.out.println("修改成功");
            Thread.sleep(1000);
            System.out.println("--------------------------");
            // 删除节点
            System.out.println("开始删除test节点");
            curatorFramework.delete().deletingChildrenIfNeeded().withVersion(-1).forPath(nodePath);
            System.out.println("删除节点完成");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    curator提供了3个中监听方式
    1.PathChildCache    监听一个节点下子节点的创建、删除、更新
    2.NodeCache         监听一个节点的更新和创建事件
    3.TreeCache         综合PathChildCache和NodeCache的所有功能
     */
    public static void addPathChildCache(CuratorFramework curatorFramework,String path) throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework,path,false);
        PathChildrenCacheListener pathChildrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("进入PathChildCache监听,监听状态为："+pathChildrenCacheEvent.getType());
            }
        };
        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
        pathChildrenCache.start();
    }
    public static void addNodeCache(CuratorFramework curatorFramework,String path) throws Exception {
        NodeCache nodeCache = new NodeCache(curatorFramework,path,false);
        NodeCacheListener nodeCacheListener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                    System.out.println("进入NodeCache监听");
            }
        };
        nodeCache.getListenable().addListener(nodeCacheListener);
        nodeCache.start();
    }
    public static void addTreeCache(CuratorFramework curatorFramework,String path) throws Exception {
        TreeCache treeCache = new TreeCache(curatorFramework,path);
        TreeCacheListener treeCacheListener =new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println("进入TreeCache监听,监听状态为："+treeCacheEvent.getType());
            }
        };
        treeCache.getListenable().addListener(treeCacheListener);
        treeCache.start();
    }
}