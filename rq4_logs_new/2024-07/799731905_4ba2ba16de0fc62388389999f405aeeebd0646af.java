package dataStructure.p24511;

import dataStructure.DataStructure;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayDeque;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setIn(new FileInputStream(DataStructure.PATH + "/p24511/data/1.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        // 객체 안에 파일 IO 권한을 주지 않는다.
        // 외부에 의존성을 최대한 방지
        QueueStack qs = new QueueStack(br.readLine(), br.readLine(), br.readLine());
        qs.addNums(br.readLine(), br.readLine());
        System.out.print(qs.answer());
    }
}

class QueueStack {

    // QueueStack 클래스안에서 객체를 1번만 생성하면 final로 생성
    private final ArrayDeque<Integer> deque; // < final
    private final StringBuilder answer; // < final

    // 생성자를 통해서 객체에서 필요한 데이터를 받는다.
    // 생성자는 데이터를 받는 로직만 실행
    public QueueStack(String _size, String structureTypes, String elements) {

        String[] types = structureTypes.split(" ");
        String[] nums = elements.split(" ");

        int size = Integer.parseInt(_size);
        this.deque = new ArrayDeque<>(nums.length);
        for (int i = 0; i < size; i++) {
            int type = Integer.parseInt(types[i]);

            assert 0 <= type && type <= 1 : "데이터 구조 타입 stack 1, deque 0";
            if (type == 1) continue; // 스택 PASS

            deque.addLast(Integer.parseInt(nums[i])); // 데이터 넣기
        }

        this.answer = new StringBuilder();
    }

    // 다형성 오버로딩 사용 (데이터 타입 변경 위해서 사용)
    public void addNums(String inputCount, String _data) {
        addNums(Integer.parseInt(inputCount), _data); // 데이터 타입 변경
    }

    // 들어갈 번호 데이터 데이터 1줄 읽기
    public void addNums(int inputCount, String _data) {
        String[] data = _data.split(" ");
        for (int i = 0; i < inputCount; i++) {
            int out = add(Integer.parseInt(data[i]));
            answer.append(out);
            answer.append(" ");
        }
    }

    // 번호 데이터 1개 읽기
    private int add(int num) {
        deque.addFirst(num); // 1개 넣기
        return deque.removeLast(); // 1개 빼기
    }

    public String answer() {
        return answer.toString();
    }
}