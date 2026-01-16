import java.util.*;

class HighScores {
    private List<Integer> highScores;
    private List<Integer> originalScores;
    public HighScores(List<Integer> highScores) {
        this.highScores = highScores;
        this.originalScores = new ArrayList<>(highScores);
    }

    List<Integer> scores() {
        return this.originalScores;
    }

    Integer latest() {
        //highScores.get(highScores.size()-1)
        return this.originalScores.getLast();
    }

    Integer personalBest() {
        int best = 0 ;
        // for (int i = 0; i < highScores.size(); i++) {
        for (Integer highScore : this.highScores) {
            if (highScore > best) {
                best = highScore;
            }
        }
        return best;
        //return  Collections.max(highScores);
    }

    List<Integer> personalTopThree() {
        Collections.sort(highScores);
        Collections.reverse(highScores);
        if (highScores.size()<3){return highScores.subList(0,highScores.size());}
        return this.highScores.subList(0,3);
    }

}