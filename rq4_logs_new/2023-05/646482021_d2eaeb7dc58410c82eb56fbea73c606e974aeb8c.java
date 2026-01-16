import java.util.List;

public class Controller {
    private Cart cart = new Cart();
    private List<Menu> menuList = List.of(
            new Menu(1, "불고기버거", 4000, "버거", "맛있는 불고기버거입니다.", 2),
            new Menu(2, "치즈버거", 4000, "버거", "맛있는 치즈버거입니다.", 2),
            new Menu(3, "한우불고기버거", 4000, "버거", "맛있는 한우불고기버거입니다.", 2),
            new Menu(4, "감자튀김", 4000, "사이드메뉴", "맛있는 감자튀김.", 2));
    private Panel panel = new Panel();

    void powerOn() {
        panel.printPowerOn();
    }

    void powerOff() {
        panel.printPowerOff(); // panel.print("종료됩니다.")와 동일
    }

    int run() {
        int option = panel.printStartOption();
        switch (option) {
            case 1:
                panel.print("주문하기 로직");
                break;
            case 2:
                panel.print("장바구니 보기 로직");
                break;
            case 3:
                panel.print("결제하기 로직");
                break;
            case 4:
                powerOff();
                break;
        }
        return option;
    }
}