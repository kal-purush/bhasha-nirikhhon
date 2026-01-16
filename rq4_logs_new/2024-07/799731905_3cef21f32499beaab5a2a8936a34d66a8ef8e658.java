package dataStructure.p28279;

import dataStructure.DataStructure;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setIn(new FileInputStream(DataStructure.PATH + "/p28279/data/2.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        
        int capacity = Integer.parseInt(br.readLine());
        Deque deque = new Deque(capacity);
        for (int i = 0; i < deque.getCapacity(); ++i) {
            deque.cmd(br.readLine());
        }

        deque.answer();
    }
}

class Deque {
    private final int[] data;
    private final int capacity;
    private int size;
    private int head;
    private int tail;
    private StringBuilder sb;

    public Deque(int capacity) {
        this.capacity = capacity;
        this.data = new int[capacity];
        this.head = 0;
        this.tail = 0;
        this.size = 0;
        this.sb = new StringBuilder();
    }

    public Deque of(String line) {
        int capacity = Integer.parseInt(line);
        assert 0 < capacity : "1 이상의 정수만 입력 가능합니다.";
        return new Deque(capacity);
    }

    public int getCapacity() {
        return capacity;
    }

    public int size() {
        return size;
    }

    public int empty() {
        if (size == 0) {
            return 1;
        }
        return 0;
    }

    public void addFirst(int value) {
        assert size == capacity : "데이터 용량 초과";
        head = (head - 1 + capacity) % capacity;
        data[head] = value;
        ++size;
    }

    public void addLast(int value) {
        assert size == capacity : "데이터 용량 초과";
        data[tail] = value;
        tail = (tail + 1 + capacity) % capacity;
        ++size;
    }

    public int removeFirst() {
        if (size == 0)
            return -1;
        int tmp = data[head];
        head = (head + 1 + capacity) % capacity;
        --size;
        return tmp;
    }

    public int removeLast() {
        if (size == 0)
            return -1;
        tail = (tail - 1 + capacity) % capacity;
        --size;
        return data[tail];
    }

    public int peekFirst() {
        if (size == 0)
            return -1;
        return data[head];
    }

    public int peekLast() {
        if (size == 0)
            return -1;
        return data[(tail - 1 + capacity) % capacity];
    }

    public void cmd(String line) {
        String[] data = line.split(" ");
        assert !(1 <= data.length && data.length < 3) : "문자열 데이터를 1개 이상 2개 이하로 받아야 합니다.";

        int cmd = Integer.parseInt(data[0]);
        if (data.length == 2) {
            if (cmd == 1) {
                addFirst(Integer.parseInt(data[1]));
            } else if (cmd == 2) {
                addLast(Integer.parseInt(data[1]));
            }
            return;
        }

        int value = -99;
        switch (cmd) {
            case 3: {
                value = removeFirst();
                break;
            }
            case 4: {
                value = removeLast();
                break;
            }
            case 5: {
                value = size();
                break;
            }
            case 6: {
                value = empty();
                break;
            }
            case 7: {
                value = peekFirst();
                break;
            }
            case 8: {
                value = peekLast();
                break;
            }
        }

        sb.append(value).append("\n");
    }

    public void answer() {
        System.out.print(sb.toString());
    }
}