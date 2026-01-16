package headFirst.adapter;

public class DuckTestDrive {
    public static void main(String[] args) {
        Turkey turkey = new WildTurkey();
        Duck duck = new MallardDuck();

        Duck turkeyAdapter = new TurkeyAdapter(turkey);

        System.out.println("The turkey says...");
        turkey.gobble();
        turkey.fly();

        System.out.println("The mallard duck says...");
        testDuck(duck);

        System.out.println("The adapted wild turkey says...");
        testDuck(turkeyAdapter);
    }

    public static void testDuck(Duck duck) {
        duck.quack();
        duck.fly();
    }
}