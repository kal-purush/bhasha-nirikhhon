package dataStructure.p2346;

import dataStructure.DataStructure;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayDeque;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setIn(new FileInputStream(DataStructure.PATH + "/p2346/data/1.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        Ring ring = Ring.of(br.readLine());
        ring.input(br.readLine());
        ring.popStart();
        ring.answer();
    }
}

class Ring {

    private final ArrayDeque<Balloon> deque;
    private final StringBuilder sb;

    public Ring(int capacity) {
        this.deque = new ArrayDeque<>(capacity);
        this.sb = new StringBuilder(capacity);
    }

    public static Ring of(String line) {
        return new Ring(Integer.parseInt(line));
    }

    public void input(String line) {
        String[] pages = line.split(" ");
        for (int num = 0; num < pages.length; num++) {
            deque.addLast(new Balloon(num + 1, Integer.parseInt(pages[num])));
        }
    }

    public void popStart() {
        while (true) {
            Balloon balloon = deque.removeFirst(); // 첫번째 뽑기
            sb.append(balloon.getNumber()).append(" ");

            if (deque.isEmpty())
                return;

            int control = balloon.pop();
            int move = Math.abs(control);

            if (control < 0) { // 왼쪽 돌리기
                for (int i = 0; i < move; i++) {
                    deque.addFirst(deque.removeLast());
                }
            } else { // 오른쪽 돌리기
                for (int i = 0; i < move  - 1; i++) {
                    deque.addLast(deque.removeFirst());
                }
            } // 오른쪽
        }
    }

    public void answer() {
        System.out.print(sb);
    }
}

class Balloon {

    private int number;
    private int paper;

    public Balloon(int number, int paper) {
        this.number = number;
        this.paper = paper;
    }

    public int getNumber() {
        return number;
    }

    public int pop() {
        return paper;
    }

    @Override
    public String toString() {
        return "Balloon{" +
                "number=" + number +
                ", paper=" + paper +
                '}';
    }
}