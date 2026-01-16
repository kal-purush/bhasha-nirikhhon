public class Class_12 {
    // 객체지향 프로그래밍 : 객체 간의 관계를 표현하는 것을 중점으로 둠.

    // 객체지향 프로그래밍의 특징
    // 1. 캡슐화 (정보 은닉) : 겉으로는 깔끔하게, 로직에 필요한 정보들은 모두 클래스 내부에.
    // 2. 클래스 : 클래스는 기능과 속성으로 구성되어 있음. 기능을 통해 속성을 변경하도록 접근한다.
    // 3. 인터페이스
    // 4. 추상화 : 복잡한 것을 단순화.
    // 5. 다형성 : 똑같은 객체가 다양한 형태를 띈다. (재사용)

    // 정보 은닉을 위해 접근 제어자를 사용.

    // 클래스 내부 : 필드, 메서드, 생성자, 내부 클래스
    // 클래스 외부 : 패키지, import, 외부 클래스

    // 필드(속성, 변수) : 인스턴스 필드, static 필드
    // 메서드(기능, 함수) : 인스턴스 필드, static 필드

    // 생성자 특징 : 메서드는 아님. 객체생성 기능. 클래스와 이름이 동일. 반환형이 없음.
    // 생성자 : 디폴트 생성자, 커스텀 생성자

    // 내부 클래스 : 클래스 내부에 존재하는 클래스. 잘 안 쓰임.
    // 중첩 클래스(nested class) : 인스턴스 내부 클래스, static 내부 클래스(nested inner class)
    // 지역 내부 클래스(local inner class) : 메서드 내부에 들어가는 클래스 (*자주 사용*)
    // 익명 내부 클래스(anonymous inner class) : 이름이 없는 클래스. (*자주 사용*) 인터페이스랑 자주 사용.

    // 상속 : 맴버만 상속. 맴버는 '필드, 메서드, 이너클래스'
    // 패키지 : 폴더 그룹화
    // import : 패키지의 경로를 추가.


    public static void main(String[] args) {
        Book book1 = new Book();
        book1.show_title();

        Book book2 = new Book();
        // private 로 선언한 title 은 인스턴스에서 접근이 불가.
        book2.writer = "자까자까";
        book2.price = 2000;
        book2.show_title();
        book2.show_writer();

        Book book3 = new Book("title", "writer", 100000);
        book3.show_title();
        book3.show_writer();
    }
}

class Book {
    private String title;
    String writer;
    int price;

    // 디폴트 생성자
    Book() {
        this.title = "홍길동전";
        this.writer = "허균";
        this.price = 10000;
    }

    // 메서드 오버로딩 : 하나의 클래스 안에 같은 이름의 메서드를 여러 개 정의. 매개변수의 개수 또는 타입이 다를 경우에만 가능
    // 커스텀 생성자
    Book(String title, String writer, int price) {
        this.title = title;
        this.writer = writer;
        this.price = price;
    }

    void show_title() {
        System.out.println(title);
    }

    void show_writer() {
        System.out.println(writer);
    }
}