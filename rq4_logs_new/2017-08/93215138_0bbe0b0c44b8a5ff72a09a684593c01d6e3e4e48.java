package gs6.nekisse.exercise;

import java.util.Scanner;

/**
 * Created by Nekisse_lee on 2017. 8. 13..
 */
public class Admin {
    Scanner scanner = new Scanner(System.in);
    private Pos pos;
    private Menu menu = new Menu();
    public void start() {
        System.out.println("관리자 모드입니다. \n 메뉴를 선택해 주세요.");
        System.out.println("1.메뉴 추가,삭제 | 2. 메뉴 조회 | 3. 매출 조회 | 4. 남은 잔고 | 5. 종료" );
        boolean run = true;
        while (run){
           int selectnum= scanner.nextInt();
            switch (selectnum){
                case 1:
                    break;
                case 2:
                    search();
                    break;
                case 3:
                    break;
                case 4:
                    break;
                case 5:
                    run = false;
                    break;

            }
        }
    }

    private void search() {
        menu.findAll().stream().map(MenuItem::toString).forEach(System.out::println);
    }



}