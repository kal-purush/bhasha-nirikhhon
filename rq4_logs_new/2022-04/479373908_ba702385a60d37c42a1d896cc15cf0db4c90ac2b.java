import java.util.ArrayList;

public class Bunker extends DefaultCritter {
    public void print(){
        StdDraw.filledSquare(x,y,0.01);
    }
    boolean isHit(ArrayList<Missile> m){
        boolean hit=false;
        for (int k =0; k<m.size();k++){
            double mx= m.get(k).getX();
            double my= m.get(k).getY();
            double dist = Math.sqrt(Math.pow(x-mx,2)+Math.pow(y-my,2));
            if (dist < 0.01)
            {
                hit =true;
                System.out.println("Bunker hit!");
                m.remove(k);
            }
        }
        return hit;
    }
}