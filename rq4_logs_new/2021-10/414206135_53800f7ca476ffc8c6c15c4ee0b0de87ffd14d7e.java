package com.msh.bird;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;

/**
 * @author: lgd
 * @date: 2021/09/19/ 9:55
 * 小鸟信息类
 */
public class Bird extends Global{
    public int size;
    public BufferedImage[] images;
    public int index;

    //计算小鸟的位置
    /**
     * 重力加速度
     */
    public double g;
    /**
     * 两次位置的间隔时间
     */
    public double t;
    /**
     * 初始上抛速度
     */
    public double v0;
    /**
     * 是当前的上抛速度
     */
    public double speed;
    /**
     * 是经过时间t以后的位移
     */
    public double s;
    /**
     * 是鸟的倾角 弧度单位
     */
    public double angle;
    /**
     * 对于实例变量进行初始化操作
     */
    public Bird() throws Exception{
        //小鸟默认是第一张图片
        image= ImageIO.read(getClass().getResource("img/0.png"));// 如果名字写错都会出现运行异常
        width=image.getWidth();
        height=image.getHeight();
        x=350;
        y=250;
        size=40;
        g=4;
        v0=20;
        t=0.25;
        speed=v0;
        s=0;
        angle=0;
        // 创建小鸟图片数组对象，8张图片
        images = new BufferedImage[8];
        for (int i = 0; i < 8; i++) {
            images[i] = ImageIO.read(getClass().getResource("img/" + i + ".png"));
        }
        // 初始默认是第一张小鸟的图片
        index = 0;
    }

    /**
     * 小鸟飞翔的方法
     */
    public void fly(){
        index++;
        //让8张图片进行轮播
        image=images[(index / 12) % 8];
    }


    public static void main(String[] args) {
        System.out.println((8 / 12) % 8);
    }











}