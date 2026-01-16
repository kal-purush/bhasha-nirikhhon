package lesson1;

public class Team{
    private String teamName;
    private Animal [] animals;


    public Team(String teamName, Animal[] animals) {
        this.teamName = teamName;
        this.animals = animals;
    }

    public String getTeamName() {
        return teamName;
    }

    public void showResultsForEach () {
        for (Animal anim:
             animals) {
            System.out.println(anim.getName() + " on distance " + anim.isOnDistance());
        }
    }

    public boolean showTeamResult () {
        for (Animal anim:
             animals) {
            if (anim.isOnDistance()) return true;
        }
        return false;
    }
}