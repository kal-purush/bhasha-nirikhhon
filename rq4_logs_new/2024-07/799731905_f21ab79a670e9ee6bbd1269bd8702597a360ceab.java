package stack.p4949;

import stack.StackPath;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setIn(new FileInputStream(StackPath.PATH + "/p4949/data/1.txt"));

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder sb = new StringBuilder();
        while (true) {
            String line = br.readLine();
            if (".".equals(line)) // 종료 지점
                break;
            String answer = Balance.check(line); // 균형 확인
            sb.append(answer).append("\n");
        }

        System.out.print(sb);
    }
}

class Balance {

    public static String check(String line) {
        Stack stack = new Stack(line.length()); // 스택 생성

        boolean forContinue = true; // 정상적이면 true 유지
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (!('[' == c || c == ']'
                    || '(' == c || c == ')')) continue;

            forContinue = stack.push(c);
            if (!forContinue) // 비정상이면 false 종료
                break;
        }

        if (forContinue && stack.empty())
            return "yes";
        return "no";
    }
}

class Stack {
    private final char[] array;
    private int index;

    public Stack(int size) {
        this.array = new char[size];
        this.index = 0;
    }

    public boolean push(char c) {
        if (empty()) { // 빈경우
            this.array[index++] = c;
            return true;
        }

        char top = peek();
        if ((top == '[' && ']' == c) || (top == '(' && ')' == c)) { // 닫는 경우
            pop();
            return true;
        }

        // 나머지 넣기
        this.array[index++] = c;
        return true;
    }

    public boolean empty() {
        return index == 0;
    }

    private char pop() {
        return array[--index];
    }

    private char peek() {
        return array[index - 1];
    }
}