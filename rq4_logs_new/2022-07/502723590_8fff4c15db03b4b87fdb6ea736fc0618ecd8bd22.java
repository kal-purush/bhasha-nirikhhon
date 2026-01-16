package Socet;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) {
        // Указываем порт, к которому привязывается сервер
        int serverPort = 7777;
        // IP-адрес компьютера, где исполняется наша серверная программа
        String address = "192.168.1.174"; // адрес данного компьютера
        try {
            // Создаем объект который отображает указанный IP-адрес.
            InetAddress ipAddress = InetAddress.getByName(address);
            // Создаем сокет, используя IP-адрес и порт сервера.
            try (Socket socket = new Socket(ipAddress, serverPort)) {
                System.out.println("Сервер найден!");
                // Получаем входной и выходной потоки сокета
                // Теперь можем получать и отсылать данные клиенту:
                DataInputStream in = new DataInputStream(socket.getInputStream());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                // Создаем сканнер:
                @SuppressWarnings("resource")
                Scanner scan = new Scanner(System.in);
                String line;
                while (true) {
                    System.out.print("Я: ");
                    line = scan.nextLine();
                    out.writeUTF(line); // отсылаем введенную строку текста серверу
                    out.flush(); // очищаем поток.
                    line = in.readUTF(); // ждем, пока сервер отошлет строку текста
                    System.out.println("Север: " + line);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}