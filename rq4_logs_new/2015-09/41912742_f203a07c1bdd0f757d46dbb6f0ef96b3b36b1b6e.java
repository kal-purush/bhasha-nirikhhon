package keygen;

import java.math.BigInteger;

public class PrimeCheck {
	public static BigInteger sqrt(BigInteger n){
		BigInteger a = BigInteger.ONE;
		BigInteger b = new BigInteger(n.shiftRight(5).add(new BigInteger("8")).toString());
		while(b.compareTo(a) >= 0) {
		    BigInteger mid = new BigInteger(a.add(b).shiftRight(1).toString());
		    if(mid.multiply(mid).compareTo(n) > 0) b = mid.subtract(BigInteger.ONE);
		    else a = mid.add(BigInteger.ONE);
		  }
		return a;//.subtract(BigInteger.ONE);
	}
	public static boolean PrimeCheck(BigInteger x){
		if (x.mod(BigInteger.valueOf(2)) == BigInteger.ZERO){
			return false;
		}
		else {
			boolean isPrime = true;
			for (BigInteger i = BigInteger.valueOf(3); i.compareTo(sqrt(x)) < 0; i = i.add(BigInteger.valueOf(2))){
				if (x.mod(i) == BigInteger.ZERO){
						isPrime = false;
				}
				
			}
			if (isPrime){
				return true;
			}
			else {
				return false;
			}
		}
	}
}