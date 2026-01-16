import java.util.Scanner;

public class example4 {
    public static void main(String args[]) {
        final String admin_id = "admin";            // 관리자 ID, PW 설정
        final String admin_pw = "0000";

        String user_id = "";                        // user의 ID 저장
        String user_pw = "";                        // user의 PW 저장
        String[] no_words = {"킹", "시발", "꺼저", "미친"};    // 금칙어 설정
        String[] alt_words = {"열", "사랑", "용기", "희망"};   // 대체어 설정

        Scanner sc = new Scanner(System.in);

        System.out.print("[시스템] 유튜브 계정의 아이디를 입력하세요 {Ex - admin} : ");
        user_id = sc.nextLine();
        System.out.print("[시스템] 유튜브 계정의 비밀번호를 입력하세요{Ex - 0000} : ");
        user_pw = sc.nextLine();

        if(admin_id.equals(user_id) && admin_pw.equals(user_pw)) {  // 관리자권한 로그인성공 시 자막순화 프로그램 수행
            System.out.println("[안내] 안녕하세요 admin님.");
            System.out.println("[안내] 유튜브 영상의 자막을 생성해 주세요.");

            String input = sc.nextLine();                // user의 자막 입력값 저장

            System.out.println("========================================");
            System.out.println("[알림] 프로그램의 금칙어 리스트입니다.");

            for(int i = 0; i < no_words.length; i++) {              // 금칙어 리스트 출력
                System.out.print(no_words[i] + ", ");
            }
            System.out.println();
            System.out.println("========================================");
            System.out.println("[알림] 자막 순화 프로그램 결과입니다.");

            for(int i = 0; i < no_words.length; i++) {             // 금칙어가 존재하면 대체어로 변경
                input = input.replace(no_words[i], alt_words[i]);
            }
            System.out.println(">>>" + input);
            System.out.println("[안내] 프로그램을 종료합니다.");
            sc.close();

        }

        // 로그인정보가 틀린경우 재확인 메세지출력 후 종료
        else {
            System.out.println("[경고] 유튜브 계정의 아이디 및 비밀번호를 다시 확인해 주세요.");
            return ;
        }



    }
}