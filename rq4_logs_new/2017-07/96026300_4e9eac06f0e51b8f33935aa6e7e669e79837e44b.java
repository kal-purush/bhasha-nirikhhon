package cn.crepusculo.model;

import java.util.Random;

/**
 * <h1>modify class name here</h1>
 * cn.crepusculo.model
 * <p>
 * add description here
 *
 * @author Crepusculo
 * @version 1.0
 * @since 2017/7/9
 */

public class RandomData {
    static Random random = new Random();
    protected static String subTitle(){
        String[] where = {"在街上","在澳大利亚","今早突然","在明天将会","在十年前","在校医院","再也没有机会"};
        String[] what = {"被枪击","被十名混混殴打","停止呼吸","被吊起来打","脱掉自己的裤子","把同学的暑假作业喂了狗"};
        return person()
                +where[(int)(random.nextDouble() * where.length)]
                +what[(int)(random.nextDouble() * what.length)];
    }
    protected static String title(){
        String[] t = {"震惊!","不转就晚了!","男默女泪","上亿中国人都看了","没想到真相竟然是...","道德沦丧!","特朗普独家爆料:"};
        return t[(int)(random.nextDouble() * t.length)];
    }

    protected static String person(){
        String[] t = {"小明","小红","小智障","你老婆","热心市民刘先生","学十的宿舍","李瑞东","韩国某明星","WilliamAngry","Queen Vellonica"};
        return t[(int)(random.nextDouble() * t.length)];
    }
}