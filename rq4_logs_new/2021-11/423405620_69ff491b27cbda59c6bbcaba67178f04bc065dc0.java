package ruangong.our_land.model.spirit.own;

import ruangong.our_land.model.spirit.Spirit;

/**
 * @Author:HuangZhiquan
 * @author:wizardk
 * @Description:用户拥有精灵类（包含初始给予用户的精灵）
 * @Date:Created in 2021-11-19 19:20
 * @Modified By:
 */
public abstract class Own extends Spirit {
    /**
     * 用户初始精灵的初始等级，设为1
     */
    protected static final int INITIAL_LEVEL = 1;

    /**
     * 精灵属性
     */
    private String name;
    private int id;
    private int level;
    private int blood;
    private int attack;
    private int defense;
    private int speed;
    private String type;
    private String nature;

    /**
     * 用户精灵经验值(初始为0)
     */
    private int exp = 0;

    //构造方法
    public Own(String name, String id, int level, int blood, int attack, int defense, int speed, String type, String nature,int isRare) {
        super(name, id, level, blood, attack, defense, speed, type, nature,isRare);
    }

    /**
     * 精灵升级策略：精灵当前等级 = 累计经验值 / 100
     * 有必要的等级判断
     */
    public void levelUP() {
        if (this.level < MAX_LEVEL){
            int upNum = exp / 100;
            if(this.level + upNum <= MAX_LEVEL){
                int currentLevel = this.level + upNum;
                this.level = currentLevel;
            }else if(this.level + upNum > MAX_LEVEL){
                this.level = MAX_LEVEL;
            }
        }else{
            this.level = MAX_LEVEL;
        }
    }

    /**
     * 更新精灵经验累计值，用于更新等级
     * @param gain 击败野怪或boss后所获得的经验值
     */
    public void newExp(int gain){
        this.exp = this.exp + gain;
    }

}