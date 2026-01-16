public class Menu {
    private int menuNum;
    private String name;
    private int price;
    private String description;
    private int stockCount;

    // 메뉴 정보 전달
    // toString과 동일한 역할
    String showMenuInfo() {
        return "";
    }

    // 간단한 메뉴 정보 전달
    // 영수증에 리턴할때, 더 위의 showMenuInfo보다 더 간략한 정보 전달하고자 생성했는데 불필요하면 지워도 O
    String showShortenMenuInfo() {
        return "";
    }

    // Cart에서 totalAmount를 더해주기 위해 price가 필요한데, private이기 때문에 get 함수 추가
    int getMenuPrice() {
        return 0;
    }

    // 재고 확인
    boolean checkStock() {
        return false;
    }
}