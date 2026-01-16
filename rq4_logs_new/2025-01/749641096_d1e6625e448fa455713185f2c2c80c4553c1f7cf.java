package lang.wrapper.test;

import java.util.Random;

public class LottoGenerator1 {
    private final Random random = new Random();
    private int[] lottoNumbers;
    private int count;

    public int[] generate(){
        lottoNumbers = new int[6];
        count = 0;

        while (count < 6){
            int number = random.nextInt(45) + 1;
            if(check(number)){
                lottoNumbers[count] = number;
                count++;
            }
        }
        return lottoNumbers;
    }
    // 중복번호 검사
    public boolean check(int number){
        for(int i = 0; i < count; i++){
            if(lottoNumbers[i] == number){
                return false;
            }
        }
        return true;
    }
}