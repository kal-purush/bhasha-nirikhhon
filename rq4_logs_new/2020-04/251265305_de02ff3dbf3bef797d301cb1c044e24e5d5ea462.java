package class_03;

public class Runner {
    public static void main(String[] args) {
        //Creating two instances of Engineers
        ManualEngineer manualEngineer = new ManualEngineer();
        AutomationEngineer automationEngineer = new AutomationEngineer();

        //Fot manual engineer
        manualEngineer.setAge(30);manualEngineer.setName("Peter");manualEngineer.setSurName("Parker");
        System.out.println(manualEngineer.getAge());
        System.out.println(manualEngineer.getName()+" "+manualEngineer.getSurName());
        manualEngineer.speak();
        manualEngineer.inventCode();
        manualEngineer.executeTest();

        //Fot automation engineer
        automationEngineer.setAge(40);automationEngineer.setName("Bruce");automationEngineer.setSurName("Banner");
        System.out.println(automationEngineer.getAge());
        System.out.println(automationEngineer.getName()+" "+automationEngineer.getSurName());
        automationEngineer.speak();
        automationEngineer.inventCode();
        automationEngineer.executeTest();
    }
}