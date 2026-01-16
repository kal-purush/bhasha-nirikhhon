import client.RMIClient;
import client.Registry;

import java.io.IOException;
import java.util.Scanner;

public class MyRMIClientService {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Registry registry = RMIClient.createRegistry(10001);
        RMIServiceCalculator service = (RMIServiceCalculator) registry.lookup("service/calculator");
        Scanner scanner = new Scanner(System.in);
        System.out.println(service.sub(scanner.nextInt(),scanner.nextInt()));
        System.out.println();
    }
}