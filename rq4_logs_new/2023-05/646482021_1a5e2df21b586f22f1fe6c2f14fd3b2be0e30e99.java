import java.util.Scanner;

public class Panel {
    Scanner sc = new Scanner(System.in);

    int printStartOption() {
        System.out.print("--------선택지---------\n" +
                "1번: 주문하기\n" +
                "2번: 장바구니 보기\n" +
                "3번: 결제하기\n" +
                "4번: 종료하기\n" +
                "----------------------\n" +
                "실행할 메뉴의 번호를 입력해주세요: ");
        return sc.nextInt();
    }

    void printPowerOn() {
        System.out.println("어서오세요 햄버거 가게입니다~🍔");
    }

    void printPowerOff() {
        System.out.println("키오스크를 종료합니다.");
    }
    void print(String str) {
        System.out.println(str);
    }
}