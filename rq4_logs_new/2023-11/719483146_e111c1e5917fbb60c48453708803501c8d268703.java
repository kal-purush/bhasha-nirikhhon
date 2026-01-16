package BasicsRefresh.src;

public class Operators {
    public static void main(String[] args) {

        System.out.println("Hello world!");
        double myFirstValue = 20.00d;
        double mySecondValue = 80.00;
        double myTotalValue = ( myFirstValue + mySecondValue ) * 100.00d;
        System.out.println("My Total value :"+ myTotalValue);
        double myReminder = myTotalValue % 40.00d;
        boolean isReminderAvailable = (myReminder == 0) ? true : false;
        if (!isReminderAvailable){
            System.out.println("Got Some reminder");
        }

    }
}