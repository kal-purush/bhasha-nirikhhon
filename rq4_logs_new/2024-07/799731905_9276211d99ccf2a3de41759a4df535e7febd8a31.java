package stack.p28278;

import stack.StackPath;

import java.io.*;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setIn(new FileInputStream(StackPath.PATH + "/p28278/data/1.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        Stack stack = Stack.of(br.readLine());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < stack.getSize(); i++) {
            String[] data = br.readLine().split(" ");
            if (data.length == 2) { // 데이터 추가
                stack.push(Integer.parseInt(data[1]));
                continue;
            }
            int cmd = Integer.parseInt(data[0]);

            switch (cmd) {
                case 2: {
                    sb.append(stack.pop());
                    break;
                }
                case 3: {
                    sb.append(stack.size());
                    break;
                }
                case 4: {
                    sb.append(stack.empty());
                    break;
                }
                case 5: {
                    sb.append(stack.peek());
                    break;
                }
            }
            sb.append("\n");
        }
        System.out.println(sb);
    }
}

class Stack {
    private final int[] array;
    private final int size;
    private int index;

    public static Stack of(String line) {
        return new Stack(Integer.parseInt(line));
    }

    public Stack(int size) {
        this.size = size;
        this.array = new int[size];
        this.index = -1;
    }

    // 데이터 넣기
    public void push(int value) {
        array[++index] = value;
    }

    // 데이터 빼기
    public int pop() {
        if (index == -1) {
            return index;
        }
        return array[index--];
    }

    // 데이터 개수 출력
    public int size() {
        return index + 1;
    }

    // 데이터 비어있는지 확인
    public int empty() {
        if (index == -1) {
            return 1;
        }
        return 0;
    }

    // 맨위확인
    public int peek() {
        if (index == -1) {
            return index;
        }
        return array[index];
    }

    public int getSize() {
        return size;
    }
}