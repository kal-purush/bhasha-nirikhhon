public class JadenCase {

    public static void main(String[] args) {

    }

    public String solution(String s) {

        String[] words = s.split(" ");
        StringBuilder result = new StringBuilder();
        for (String word : words) {
            if (word.length() > 0){
            result.append(word.substring(0, 1).toUpperCase())
                .append(word.substring(1).toLowerCase());
            }
            result.append(" ");
        }

        if(s.charAt(s.length() - 1) == ' ') return result.toString();
        return result.toString().trim();
    }
}