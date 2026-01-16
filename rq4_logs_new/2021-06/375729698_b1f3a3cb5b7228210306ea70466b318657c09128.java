public class Calc {

	// Формирование ежемесячного платежа
	public int getMonthlyPay(int sum, int months, int rates) {
		// ежемесячный процент
		double rate = (double) rates / 100 / 12;
		// коэффициент аннуитета
		double coefficient = (rate * Math.pow((1  + rate), months) / ( Math.pow((1 + rate), months ) - 1));
		return (int) Math.round(sum * coefficient);
	}

	//Переплата за весь период
	public int getOverpayAmount(int monthPay, int months, int sum) {
		return (monthPay * months) - sum;
	}

	//Общая сумма к возврату в банк
	public int getTotalPay(int sum, int overPay) {
		return sum + overPay;
	}
}