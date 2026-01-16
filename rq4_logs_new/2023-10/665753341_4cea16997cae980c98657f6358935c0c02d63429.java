package code.test.ch05;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

/**
 * ch05. Stack, Queue (자료구조)
 * : 공주 구하기
 *
 * 설명
 * 정보 왕국의 이웃 나라 외동딸 공주가 숲속의 괴물에게 잡혀갔습니다.
 * 정보 왕국에는 왕자가 N명이 있는데 서로 공주를 구하러 가겠다고 합니다.
 * 정보왕국의 왕은 다음과 같은 방법으로 공주를 구하러 갈 왕자를 결정하기로 했습니다.
 * 왕은 왕자들을 나이 순으로 1번부터 N번까지 차례로 번호를 매긴다.
 * 그리고 1번 왕자부터 N번 왕자까지 순서대로 시계 방향으로 돌아가며 동그랗게 앉게 한다.
 * 그리고 1번 왕자부터 시계방향으로 돌아가며 1부터 시작하여 번호를 외치게 한다.
 * 한 왕자가 K(특정숫자)를 외치면 그 왕자는 공주를 구하러 가는데서 제외되고 원 밖으로 나오게 된다.
 * 그리고 다음 왕자부터 다시 1부터 시작하여 번호를 외친다.
 * 이렇게 해서 마지막까지 남은 왕자가 공주를 구하러 갈 수 있다.
 *
 * 예를 들어 총 8명의 왕자가 있고, 3을 외친 왕자가 제외된다고 하자. 처음에는 3번 왕자가 3을 외쳐 제외된다.
 * 이어 6, 1, 5, 2, 8, 4번 왕자가 차례대로 제외되고 마지막까지 남게 된 7번 왕자에게 공주를 구하러갑니다.
 * N과 K가 주어질 때 공주를 구하러 갈 왕자의 번호를 출력하는 프로그램을 작성하시오.
 *
 * 입력
 * 첫 줄에 자연수 N(5<=N<=1,000)과 K(2<=K<=9)가 주어진다.
 *
 * 출력
 * 첫 줄에 마지막 남은 왕자의 번호를 출력합니다.
 *
 * 예시 입력 1
 * 8 3
 *
 * 예시 출력 1
 * 7
 */
public class SaveThePrincess
{
	public static void main(String[] args)
	{
		SaveThePrincess saveThePrincess = new SaveThePrincess();
		Scanner scanner = new Scanner(System.in);
		int n = scanner.nextInt();
		int k = scanner.nextInt();
		int result = saveThePrincess.solution(n, k);
		System.out.print(result);
		
		System.out.println();
		int result2 = saveThePrincess.solution2(n, k);
		System.out.println("result2 = " + result2);
	}
	
	public int solution(int n, int k)
	{
		Queue<Integer> queue = new LinkedList<>();
		
		for (int i = 1; i <= n; i++)
		{
			queue.add(i);
		}
		
		int cnt = 1;
		while (queue.size() != 1)
		{
			if (cnt != k) {
				Integer poll = queue.poll();
				queue.offer(poll);
				cnt++;
			}else {
				queue.poll();
				cnt = 1;
			}
		}
		
		return queue.element();
	}
	
	public int solution2(int n, int k)
	{
		int answer = 0;
		Queue<Integer> queue2 = new LinkedList<>();
		for (int i = 1; i <=n; i++)
		{
			queue2.offer(i);
		}
		
		while (!queue2.isEmpty())
		{
			for (int i = 1; i <k; i++)
			{
				queue2.offer(queue2.poll()); // poll() : 꺼내고 그 꺼낸 값을 리턴한다.
			}
			queue2.poll();
			
			if (queue2.size() == 1)
				answer = queue2.poll();
		}
		
		
		return answer;
	}
}