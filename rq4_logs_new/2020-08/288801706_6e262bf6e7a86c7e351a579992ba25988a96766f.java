package lesson_2_1.competition;

import lesson_2_1.competition.obstacles.Cat;
import lesson_2_1.competition.obstacles.Human;
import lesson_2_1.competition.obstacles.Obstacle;
import lesson_2_1.competition.obstacles.Robot;
import lesson_2_1.competition.participants.Participant;
import lesson_2_1.competition.participants.Treadmill;
import lesson_2_1.competition.participants.Wall;

public class Main {

    public static void main(String[] args) {
        Participant[] participants = {
                new Cat("Pushok"),
                new Human("Piter"),
                new Robot("Wall-E"),
                new Cat("Musya", 2100, 3),
                new Human ("Stive", 3500, 2),
                new Robot("T1000", 10000, 8)
        };

        Obstacle[] obstacles = {
                new Wall(1),
                new Treadmill(2000),
                new Wall(2),
                new Treadmill(1500)
        };

        for (Participant p : participants) {
            for (Obstacle o : obstacles) {
                o.doIt(p);
                if (!p.canDoIt()) {
                    break;
                }
            }

        }

        for (Participant p : participants) {
            p.info();
        }

    }

}