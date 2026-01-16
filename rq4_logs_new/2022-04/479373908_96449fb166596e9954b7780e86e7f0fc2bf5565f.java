import java.util.ArrayList;
class Enemy extends DefaultCritter {
    public static void main(String[] args) {
    }
    public void print(){
        StdDraw.filledCircle(x,y,0.04);
    }
    boolean isHit(ArrayList<Missile> m){
        boolean hit=false;
        for (int k =0; k<m.size();k++){
            double mx= m.get(k).getX();
            double my= m.get(k).getY();
            double dist = Math.sqrt(Math.pow(x-mx,2)+Math.pow(y-my,2));
            if (dist < 0.05 && m.get(k).getType()==false)
            {
                hit =true;
                System.out.println("hit!");
                m.remove(k);
            }
        }
        return hit;
    }



}