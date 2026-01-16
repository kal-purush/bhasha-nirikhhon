package class1;

public class ClassStart22 {

    public static void main(String[] args) {
        // class1보다는 나음. 배열로 관리하니까
        // 근데, 여전히 문제가 있음. 정보가 3개의 배열에 나뉘어져 있잖아
        // 그러면, 한개 학생 정보 제거하거나 추가하려면? 3개배열을 모두 수정해야함;;
        // -> 클.래.스.를 사용해야 함!
        String[] studentNames = {"A","b","c","d","e"};
        int[] studnetages = {1,2,3,4,5};
        int[] studentGrade = {11,22,33,44,55};

        for (int i=0; i<studentNames.length; i++) {
            System.out.println(studentNames[i]+studnetages[i]+studentGrade[i]);
        }
    }
}