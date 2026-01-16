package ShapesDrawing;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Scanner;

public class Main{
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        Queue<Integer> queue = new ArrayDeque<>();
        for (int i = 0; i < 5; i++) {
            queue.add(Integer.parseInt(scanner.nextLine()));
        }

        Drawable circle = new Circle(queue.poll(), queue.poll(), queue.poll());
        Drawable rect = new Rectangle(queue.poll(), queue.poll());

        printShapes(circle);
        printShapes(rect);
//        circle.draw();
//        rect.draw();
    }
    private static void printShapes(Drawable shape){
        shape.draw();
    }
}