package Hash;

import java.util.HashMap;
import java.util.Map;

public class Programmers_phoneNumberHash {
	public boolean solution(String[] phone_book) {
        Map<String, Integer> hashPhoneBook = new HashMap<>();
        for(String number: phone_book){
            hashPhoneBook.put(number, 0);
        }
        for(String number: phone_book){
            for(int i = 1; i <= number.length() - 1; i++){
                if(hashPhoneBook.containsKey(number.substring(0, i))){
                    return false;
                }
            }

        }
        return true;
    }
}