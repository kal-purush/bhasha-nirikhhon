import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/**
 * BOJ 16987: 계란으로 계란치기
 * - 메모리: 15504kb
 * - 시간: 240ms
 *
 * [설계]
 * 주어진 조건에 맞게 재귀(DFS)로 풀면 되는 문제
 * 1. 가장 왼쪽의 계란을 든다 => 0번째 위치에 있는 계란부터 시작
 * 2. 손에 들고 있는 계란으로 깨지지 않은 다른 계란 중에서 하나를 친다. => 계란 A의 내구도 - 계란 B의 무게 & vice versa
 *    단, 손에 든 계란이 깨졌거나 깨지지 않은 다른 계란이 없으면 치지 않고 넘어간다. => 재귀 분기문 조건
 *    이후 손에 든 계란을 원래 자리에 내려놓고 3번 과정을 진행한다.
 * 3. 가장 최근에 든 계란의 한 칸 오른쪽 계란을 손에 들고 2번 과정을 다시 진행한다. => 재귀 타고 내려가기 
 *    단, 가장 최근에 든 계란이 가장 오른쪽에 위치한 계란일 경우 계란을 치는 과정을 종료한다. => 재귀 종료 조건
 *
 * [후기]
 * 문제를 잘 읽고 있는 그대로 풀면 되는 문제.
 * "깨지지 않은 다른 계란이 없으면 치지 않고 넘어간다." 이 조건을 처리 안 해줘서 계속 80%에서 틀렸었다.
 * 다른 방식으로 문제를 풀어도 안 된다면 코드 싹 지우고 문제 다시 정리한 다음 처음부터 푸는 것도 방법인 거 같다ㅠㅠ
 */

public class BOJ_16987 {

    static int eggCount, maxBreakCount = Integer.MIN_VALUE;
    static Egg[] eggs;

    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st;

        eggCount = Integer.parseInt(br.readLine().trim());
        eggs = new Egg[eggCount];

        for (int i = 0; i < eggCount; ++i) {
            st = new StringTokenizer(br.readLine(), " ");
            eggs[i] = new Egg(Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken()));
        }

        hitEgg(0, 0, eggs);
        System.out.println(maxBreakCount);
    }

    // 재귀(DFS)로 계란 깨기
    // currIdx: 현재 들고 있는 계란의 위치
    // breakCount: 현재까지 깨진 계란 수
    // eggs: 계란들의 현재 상태
    static void hitEgg(int currIdx, int breakCount, Egg[] eggs) {
        if (currIdx == eggCount) { // 가장 최근에 든 계란이 가장 오른쪽에 위치한 계란일 경우, 종료
            if (breakCount > maxBreakCount) {
                maxBreakCount = breakCount;
            }
            return;
        }

        // 손에 든 계란이 깨졌거나 깨지지 않은 다른 계란이 없으면 치지 않고 넘어감
        if (eggs[currIdx].durability <= 0 || breakCount == eggCount - 1) {
            hitEgg(currIdx + 1, breakCount, eggs);
            return;
        }

        // 모든 계란을 보면서 칠 수 있는 계란인지 확인
        for (int i = 0; i < eggCount; ++i) {
            // 이미 깨진 계란이거나, 현재 들고 있는 계란이면 continue
            if (eggs[i].durability <= 0 || i == currIdx) {
                continue;
            }

            // 계란 깨기
            eggs[currIdx].durability -= eggs[i].weight;
            eggs[i].durability -= eggs[currIdx].weight;

            if (eggs[currIdx].durability <= 0) {
                ++breakCount;
            }

            if (eggs[i].durability <= 0) {
                ++breakCount;
            }

            // 가장 최근에 든 계란의 한 칸 오른쪽 계란을 들고 계속 진행
            hitEgg(currIdx + 1, breakCount, eggs);

            if (eggs[currIdx].durability <= 0) {
                --breakCount;
            }

            if (eggs[i].durability <= 0) {
                --breakCount;
            }

            eggs[currIdx].durability += eggs[i].weight;
            eggs[i].durability += eggs[currIdx].weight;
        }
    }

    static class Egg {
        int durability, weight;

        Egg(int durability, int weight) {
            this.durability = durability;
            this.weight = weight;
        }
    }

}