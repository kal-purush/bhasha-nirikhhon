import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class Main {
    public static void main(String[] args) {
        long big1 = Long.MAX_VALUE;
        long big2 = Long.MAX_VALUE;
        long result = big1 * 10;
        BigInteger bigInt1 = BigInteger.valueOf(big1);
        BigInteger resultBig = bigInt1.multiply(BigInteger.valueOf(10));

        System.out.println("big1 = " + big1);
        System.out.println("big2 = " + big2);
        System.out.println("result = " + result);

        System.out.println("bigInt1 = " + bigInt1);
        System.out.println("resultBig = " + resultBig);

        System.out.println();
        double totale = 0;
        System.out.println("totale = " + totale);
        totale += 3.4;
        System.out.println("totale = " + totale);
        totale += 3.8;
        System.out.println("totale = " + totale);

        BigDecimal totaleBig = BigDecimal.valueOf(0);
        System.out.println("totaleBig = " + totaleBig);
        totaleBig = totaleBig.add(BigDecimal.valueOf(3.4));
        System.out.println("totaleBig = " + totaleBig);
        totaleBig = totaleBig.add(BigDecimal.valueOf(3.8));
        System.out.println("totaleBig = " + totaleBig);

        BigDecimal totaleScaled = totaleBig.setScale(2, RoundingMode.HALF_EVEN);
        System.out.println("totaleScaled = " + totaleScaled);


    }

    public static BigDecimal calculateTotalAmount(BigDecimal quantity, BigDecimal unitPrice,
                                                  BigDecimal discountRate, BigDecimal taxRate) {

        BigDecimal amount = quantity.multiply(unitPrice);
        BigDecimal discount = amount.multiply(discountRate);
        BigDecimal discountedAmount = amount.subtract(discount);
        BigDecimal tax = discountedAmount.multiply(taxRate);
        BigDecimal total = discountedAmount.add(tax);

        // round to 2 decimal places using HALF_EVEN
        BigDecimal roundedTotal = total.setScale(2, RoundingMode.HALF_EVEN);
        return roundedTotal;
    }
}