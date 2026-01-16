import java.math.BigInteger;
import java.util.Random;




public class RSAKeyGenerator {

   
   private Random rand;   
   private int bitSize;
   //large random primes, modulus = p*q
   private BigInteger q;
   private BigInteger p; 
   
   private BigInteger modulus;
   
   //f(n) = (p-1)(q-1)
   private BigInteger totient;
   //1<d<totient , also d must be coprime with th toteint
   private BigInteger d;
   //modular multiplicitive inverse of d(mod totient), d*e mod toteint = 1
   private BigInteger e;
   
   public RSAKeyGenerator(int bitSize) {
      rand = new Random();
      q = BigInteger.probablePrime(bitSize/2, rand);
      p = BigInteger.probablePrime(bitSize/2, rand);
      System.out.println("first key: "+q);
      System.out.println("second key: "+p);
      modulus = p.multiply(q);
      totient = totientFunction(p, q);
      System.out.println("totient: "+totient);
      int bitsInTotient = totient.bitLength();
      d = generateD(bitsInTotient, totient);
      e = d.modInverse(totient);
      
   }
   
   private BigInteger generateD(int bitsInTotient, BigInteger totient) {
      int bitLengthOfD = 0;
      BigInteger test = null;
      do {
         bitLengthOfD = (int)(rand.nextDouble()*bitsInTotient);
         test = BigInteger.probablePrime(bitLengthOfD, rand);
      }
      while(test.compareTo(totient) >= 0 || test.compareTo(BigInteger.ONE) <= 0);
      return test;
   } 
   
   private BigInteger totientFunction(BigInteger p, BigInteger q) {
      return (p.subtract(BigInteger.ONE)).multiply(q.subtract(BigInteger.ONE));
   }
   
   public BigInteger getE() {
      return e;
   }
   
   public BigInteger getD() {
      return d;
   }
   
   public BigInteger getModulus() {
      return modulus;
   }
   
   







}