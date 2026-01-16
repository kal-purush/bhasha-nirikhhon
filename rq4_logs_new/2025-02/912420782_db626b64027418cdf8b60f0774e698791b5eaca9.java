public class string_division {
    class Solution {
        public int solution(String s) {
            int answer = 0;
            int countX = 0, countOther = 0;
            char x = s.charAt(0);

            for (int i = 0; i < s.length(); i++) {
                if (s.charAt(i) == x) {
                    countX++;
                } else {
                    countOther++;
                }


                if (countX == countOther) {
                    answer++;
                    if (i + 1 < s.length()) {
                        x = s.charAt(i + 1);
                    }
                    countX = 0;
                    countOther = 0;
                }
            }


            if (countX != countOther) {
                answer++;
            }

            return answer;
        }
    }

}