package lesson1;

public class Main1 {
    public static void main(String[] args) {

        Animal [] animals = {
                new Cat("Murzik", 3, "black", 2),
                new Dog("Reks", 10,5,2),
                new Duck("Yelli",1,4),
                new Cat("Mars",4,"white", 1)
        };

        Animal [] animals2 = {
                new Cat("Chester", 2, "red", 1),
                new Dog("Hrunya", 3,1,1),
                new Duck("Donald",2,5),
                new Dog("Pluto",7,4, 2)
        };

        Animal [] animals3 = {
                new Cat("Timoshka", 10, "red", 2),
                new Dog("Dingo", 10,5,2),
                new Duck("Gans",10,5),
                new Dog("Balto",10,5, 2)
        };

        Obstacle [] obstacles = {
                new Water(5),
                new Road(10),
                new Wall(2)
        };

        Team team1 = new Team("Pushistiki", animals);
        Team team2 = new Team("Disney", animals2);
        Team team3 = new Team("Pixar", animals3);

        Course course1 = new Course(obstacles, animals);
        Course course2 = new Course(obstacles, animals2);
        Course course3 = new Course(obstacles, animals3);

        System.out.println("Результаты команды " + team1.getTeamName());
        course1.doIt(team1);
        team1.showResultsForEach();
        System.out.println(team1.getTeamName() + " прошла полосу?: " + team1.showTeamResult());

        System.out.println("_________");
        System.out.println("Результаты команды " + team2.getTeamName());
        course2.doIt(team2);
        team2.showResultsForEach();
        System.out.println(team2.getTeamName() + " прошла полосу?: " + team2.showTeamResult());

        System.out.println("_________");
        System.out.println("Результаты команды " + team3.getTeamName());
        course3.doIt(team3);
        team3.showResultsForEach();
        System.out.println(team3.getTeamName() + " прошла полосу?: " + team3.showTeamResult());
    }
}