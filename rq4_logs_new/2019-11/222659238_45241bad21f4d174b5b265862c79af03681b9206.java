package part2;
/*
* 2. Создайте три класса Человек, Кот, Робот, которые не наследуются от одного класса.
* Эти классы должны уметь бегать и прыгать (методы просто выводят информацию о действии в консоль).
* */
public class ClassMain {
    public static void main(String[] args) {

        ClassTreadmill treadmill = new ClassTreadmill(100);
        ClassWall wall = new ClassWall(2);

        ClassSportsmens[] sportsmens =
                {
                        new ClassSportsmens("Петя", 10, 2),
                        new ClassSportsmens("Петя1", 2, 2),
                        new ClassSportsmens("Петя2", 10, 5),
                        new ClassSportsmens("Петя3", 110, 3),
                        new ClassSportsmens("Петя4", 10, 2),
                        new ClassSportsmens("Петя5", 10, 30),
                        new ClassSportsmens("Петя6", 1000, 3000)
                };

        for (int i = 0; i < 3; i++) {

            for (int number = 1; number < sportsmens.length; number++) {

                // если не снят с дистанции, то учавствует
                if (!sportsmens[number].isLost_the_race()) {
                    treadmill.run(sportsmens[number]);
                    wall.jump(sportsmens[number]);
                }

            }

        }

      ////////////////////////////////////////////////////////////
        /*
        * 4. Создайте два массива: с участниками и препятствиями, и
        * заставьте всех участников пройти этот набор препятствий. Если участник не смог пройти
        * одно из препятствий, то дальше по списку он препятствий не идет.
        *
        * */
        InterfaceAct [] act = {
                new  ClassWall(10),
                new ClassTreadmill(25),
                new  ClassWall(1),
                new ClassTreadmill(5),
                new  ClassWall(1),
                new ClassTreadmill(5),
                new  ClassWall(9),
                new ClassTreadmill(25)
        };

        ClassSportsmens[] sportsmens1 =
                {
                        new ClassSportsmens("Kristofer", 10, 2),
                        new ClassSportsmens("Kristofer1", 2, 2),
                        new ClassSportsmens("Kristofer2", 10, 5),
                        new ClassSportsmens("Kristofer3", 110, 3),
                        new ClassSportsmens("Kristofer4", 10, 2),
                        new ClassSportsmens("Kristofer5", 10, 30),
                        new ClassSportsmens("Kristofer6", 1000, 3000)
                };


        for (int number_act=0; number_act< act.length; number_act ++){

            for(int number_member = 0; number_member < sportsmens1.length; number_member ++){

                if (!sportsmens1[number_member].isLost_the_race())
                    act[number_act].act(sportsmens1[number_member]);

            }


        }


    }



}