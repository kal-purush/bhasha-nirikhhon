package keygen;

import java.math.BigInteger;
import java.util.Random;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import filemanagement.FileData;

public class Key {
	int strength;
	BigInteger start;
	BigInteger start_second;
	BigInteger p;
	BigInteger q;
	BigInteger n;
	BigInteger phi_n;
	public int key_length;
	BigInteger e = BigInteger.valueOf(65537);
	BigInteger d;
	public PublicKey publickey;
	public PrivateKey privatekey;
	
	public Key(int exp){
		this.strength = exp;
		this.start = BigInteger.valueOf(2).pow(exp).add(new BigInteger(exp, new Random()));
		this.start_second = this.start.add(BigInteger.valueOf(2).pow(exp-2));
	}
	public void pGen() {
		this.p = PrimeGen.PrimeGen(this.start);
	}
	public void qGen() {
		this.q = PrimeGen.PrimeGen(this.start_second);
	}
	public void nGen() {
		this.n = this.p.multiply(this.q);
		this.phi_n = this.n.subtract(this.p.add(this.q.subtract(BigInteger.ONE)));
		this.key_length = this.n.bitLength();
	}
	public void dGen() {
		this.d = this.e.modInverse(phi_n);
	}
	public void PublicKeyGen(){
		this.publickey = new PublicKey(n,e);
	}
	public void PrivateKeyGen(){
		this.privatekey = new PrivateKey(n,d);
	}
	private String getDateTime() {
	     DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	     Date date = new Date();
	     return dateFormat.format(date);
	 }
	public void SaveKey(){
		//Create file
		String filepath = "/home/david/keys/" + String.valueOf(this.strength)+ "_" + getDateTime();
		File file = new File(filepath);  
		try {  
		file.createNewFile();
		}
		catch (IOException e) {  
		e.printStackTrace();  
		}
		//Write to file
		try {
			FileData.Write(filepath, publickey.n.toString(),true);
			FileData.Write(filepath, publickey.e.toString(),true);
			FileData.Write(filepath, privatekey.d.toString(),true);
		} 
		catch (IOException e) {
		}
	}
}