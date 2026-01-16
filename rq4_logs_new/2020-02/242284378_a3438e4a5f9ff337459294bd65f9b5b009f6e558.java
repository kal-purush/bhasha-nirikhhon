package com.zjb.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * 客户端
 * @author zhaojianbo
 * @date 2020/2/24 22:00
 */
public class MyZkClient {

    /**
     * 服务器地址
     */
    private static final String connectString = "192.168.25.100:2181,192.168.25.101:2181,192.168.25.102:2181";

    /**
     * 超时时间
     */
    private static final Integer sessionTimeout = 2000;

    /**
     * 初始化
     * @return
     * @throws IOException
     */
    public static ZooKeeper init() throws IOException {
        ZooKeeper zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println(event.getType() + "---" + event.getPath());
                try {
                    getNodes();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return zooKeeper;
    }

    /**
     * 创建
     * @throws IOException
     */
    public static void create() throws Exception {
        ZooKeeper zClient = init();
        String isCreate = zClient.create("/demo", "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(isCreate);
    }

    /**
     * 获取节点内容
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void getNodes() throws IOException, KeeperException, InterruptedException {
        ZooKeeper zClient = init();
        List<String> nodeList = zClient.getChildren("/", true);
        for (String node : nodeList) {
            System.out.println(node);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 判断节点是否存在
     * @throws Exception
     */
    public static void exist() throws Exception {
        ZooKeeper zClient = init();
        Stat isExist = zClient.exists("/demo001", false);
        System.out.println(isExist == null ? "not exist" : "exist");
    }

    public static void main(String[] args) throws Exception {

    }
}