import java.util.Scanner;

public class GameObjectTest {

    public static void main(String[] args) {
        //Read User input
        Scanner input = new Scanner(System.in);
        GameObject object1 = new GameObject("",0,"",0 );


        //Asks the user to input
        System.out.println("Center:(x and y) ");
        String center = input.nextLine();
        object1.setCenter(center);

        System.out.println("Velocity: ");
        int velocity = input.nextInt();
        object1.setVelocity(velocity);

        //Extra line because the previous one will skip it
        System.out.println("");
        String skip = input.nextLine();

        System.out.println("State:(Dead or Alive) ");
        String state = input.nextLine();
        object1.setState(state);

        System.out.println("Rotation: ");
        double rotation = input.nextDouble();
        object1.setRotation(rotation);

        //Extra line because the previous one will skip it
       // System.out.println("");
        //String skip2 = input.nextLine();

        //Summary message
        System.out.printf("\nObject info: %s%n", object1.getGameObject());

    }
}