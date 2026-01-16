package hilleluser;

public class Obstacle implements Let {
    Integer heightWall;
    Integer distanceMill;



    public Obstacle(Integer wall, Integer mill) {
        this.distanceMill = mill;
        this.heightWall = wall;
    }

    @Override
    public boolean overcome(Participant participant) {

        if (heightWall < participant.getJumpHeight() && distanceMill < participant.getRunDistance()) {
            return true;
        }  else
            return false;
    }
}