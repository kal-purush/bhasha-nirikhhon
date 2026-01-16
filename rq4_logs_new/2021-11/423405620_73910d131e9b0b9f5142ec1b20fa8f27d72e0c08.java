package ruangong.our_land.model;

import java.util.HashMap;

/**
 * @Author:HuangZhiquan
 * @Description:精灵类
 * @Date:Created in 2021-11-02 22:57
 * @Modified By:
 */
public class Spirit {
    //精灵基本属性
    String name;
    String id;
    int degree;
    int blood;
    int attack;
    int defense;
    int speed;
    int exp;

    //技能类数组，技能数量为4
    Skill[] skills = new Skill[4];

    //构造方法
    public Spirit(String name,String id,int degree,int blood,int attack,int defense,int speed,int exp){
        this.name = name;
        this.id = id;
        this.degree = degree;
        this.blood = blood;
        this.attack = attack;
        this.defense = defense;
        this.speed = speed;
        this.exp = exp;
    }


    //内部类：技能类
    class Skill{
        //技能基本属性
        String name;
        String desr;
        int times;
        HashMap<String,String> map = new HashMap<>();

        //构造方法
        public Skill(String name,String desr,int times){
            this.name = name;
            this.desr = desr;
            this.times = times;
        }
    }


}