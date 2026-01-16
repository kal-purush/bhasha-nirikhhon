import class_03.worker.AutomationEngineer;
import class_03.worker.TestEngineer;

public class Runner {
    public static void main(String[] args) {
        //Creating two instances of Engineers
        TestEngineer manualEngineer = new TestEngineer();
        AutomationEngineer automationEngineer = new AutomationEngineer();

        //Fot manual engineer
        manualEngineer.setAge(30);manualEngineer.setName("Peter");manualEngineer.setSurName("Parker");
        System.out.println(manualEngineer.getAge());
        System.out.println(manualEngineer.getName()+" "+manualEngineer.getSurName());

        //Fot automation engineer
        automationEngineer.setAge(40);automationEngineer.setName("Bruce");automationEngineer.setSurName("Banner");
        System.out.println(automationEngineer.getAge());
        System.out.println(automationEngineer.getName()+" "+automationEngineer.getSurName());

    }
}