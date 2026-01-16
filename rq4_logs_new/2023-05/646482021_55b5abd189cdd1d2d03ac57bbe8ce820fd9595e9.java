import java.util.HashMap;

public class Payment {
    private int paymentAmount = 0;
    private PaymentType paymentOption = PaymentType.card;
    private static int orderNumber = 0; // 해당 클래스의 모든 객체 통틀어 하나만 쓰이도록 static 선언
    private HashMap<Menu, Integer> orderList = new HashMap<Menu, Integer>();
    private int cardBalance = 6000;

    private boolean pay(int amount, PaymentType type) {
        if (type == PaymentType.card) {
            if (amount > cardBalance) {
                return false;
            } else {
                cardBalance -= amount;
                return true;
            }
        } else if (type == PaymentType.coupon) {
            // 쿠폰인 경우 일단 전부 통과
            return true;
        } else {
            return false;
        }
    }

    public  String requestPayment(Cart cartInfo, PaymentType type) {
        if (pay(cartInfo.getTotalAmount(), type)) {
            orderNumber += 1;
            // Receipt 객체 생성하고 프린터
            Receipt receipt = new Receipt(cartInfo, cartInfo.getTotalAmount(), orderNumber);
            return receipt.print();
        } else {
            return "";
        }
    }
}
