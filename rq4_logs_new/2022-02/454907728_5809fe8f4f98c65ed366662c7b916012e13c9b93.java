
public class Arena {
    public Robot fight(Robot R1, Robot R2){
        while(!R1.isDead() && !R2.isDead()){
            double random = Math.random()*10;
            if(random > 5){
                R1.fire(R2); // Tir du robot1 sur le second
            }
            else {
                R2.fire(R1); // Tir du robot 2 sur le premier
            }
        }
        return R1.isDead() ?  R2 : R1;
    }
}