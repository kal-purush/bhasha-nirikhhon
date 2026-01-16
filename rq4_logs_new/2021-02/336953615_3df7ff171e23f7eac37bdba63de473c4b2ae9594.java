package character;
//import java.util.*;

public class Player extends Character{
    public Equip arms;
    public Equip armer;

    public void printInf(){
        System.out.println("名前：" + this.name);
        System.out.println("HP：" + this.hp);
        System.out.println("攻撃力：" + this.attack);
        System.out.println("防御力：" + this.defence);
        System.out.println("特殊攻撃：" + this.mAttack);
        System.out.println("特殊防御：" + this.mDefence);
        System.out.println("素早さ：" + this.speed);
        System.out.println("-------- 装備一覧 ----------");
        System.out.println("武器");
        System.out.println("名前：" + this.arms.name);
        System.out.println("タイプ：" + this.arms.type);
        System.out.println("強さ：" + this.arms.damage);
        System.out.println("防具");
        System.out.println("名前：" + this.armer.name);
        System.out.println("タイプ：" + this.armer.type);
        System.out.println("強さ：" + this.armer.damage);
    }
}