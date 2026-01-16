import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;


public class Decryption {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	
	
	/**
	 * this method just returns the private rsa key that only
	 * bob can get and use..it converts the private key in bytes from the file 
	 * into a useful privatekey.
	 * 
	 * @return privatekey that only bob knows
	 * @throws Exception
	 */
	
	private PrivateKey loadPrivateKey() throws Exception {
		File filePrivateKey = new File("privatekey.txt");
		FileInputStream fis = new FileInputStream(filePrivateKey);
		byte[] encodedPrivateKey = new byte[fis.available()];
		fis.read(encodedPrivateKey);
		fis.close();
		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(
		encodedPrivateKey);
		PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
		return privateKey;
		}
	/**
	 * This method creates a rsa public and private keypair and then saves it to a file
	 * the keys themselves are generated with rsa using a secure random number with SHA1 pairing
	 * and are then encoded with ASN1 (public key and private key) before being written to the file
	 * 
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws NoSuchProviderException
	 */
	
public static  void generateRSA() throws NoSuchAlgorithmException, IOException, NoSuchProviderException{
		
		KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
		
		SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");
		
		keyGen.initialize(1024, random);
      final KeyPair key = keyGen.generateKeyPair();
           
     
        PrivateKey privateKey = key.getPrivate();
		PublicKey publicKey = key.getPublic();
		// Store Public Key.\
		
	
		File file = new File("F:\\Users\\Garett\\workspace\\CS460\\publickey.txt");
		FileOutputStream fop = new FileOutputStream(file);
		// if file doesnt exists, then create it
		if (!file.exists()) {
			System.out.println("supposedly file doesnt exists");
			file.createNewFile();
		}
		
		X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec( //ASN1 encoding for public keys
				publicKey.getEncoded());
				
				fop.write(x509EncodedKeySpec.getEncoded());
				fop.close();
		
	
		
	
		// Store Private Key.
				
		File file2 = new File("F:\\Users\\Garett\\workspace\\CS460\\privatekey.txt");
		FileOutputStream fos = new FileOutputStream(file2);
		// if file doesnt exists, then create it
		if (!file2.exists()) {
			System.out.println("supposedly file doesnt exists");
			file2.createNewFile();
		}
		
		PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec( //ASN1 encoding for private keys
				privateKey.getEncoded());
				
				fos.write(pkcs8EncodedKeySpec.getEncoded());
				fos.close();
		
		
		
	}

	
	/**
	 * this method is responsible for decrypting the encrypted key that alice sent him
	 * BOB needs to first decrypt the key before he can decrypt the data
	 * once BOB gets the original aes key he now can decrypt the data...this key 
	 * is then sent to the decrypt(algorithm, key) method
	 * @param RSAKey sent from Alice
	 * @return the original AES key that alice originally encrypted her data with
	 * @throws Exception
	 */
	
	SecretKey decryptAESKey(byte[] RSAKey ) throws Exception
    {
        SecretKey key = null;
        Cipher cipher = null;
        try
        {
        	Encryption eobj = new Encryption();
        	
    		
            // initialize the cipher...
            cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
            cipher.init(Cipher.DECRYPT_MODE,loadPrivateKey() );
            // generate the aes key!
            key = new SecretKeySpec ( cipher.doFinal(RSAKey), "AES" );
        }
        catch(Exception e)
        {
            System.out.println ( "exception decrypting the aes key: " 
                                                   + e.getMessage() );
            return null;
        }
        
        String decryptedKey = Base64.getEncoder().encodeToString(key.getEncoded());
        String bobsprivatekey = Base64.getEncoder().encodeToString(loadPrivateKey().getEncoded());
        
        System.out.println("Bob: decrypting the key that alice encrypted using my private rsa key: " + bobsprivatekey);
        System.out.println("Bob: decrypted aes key that should match alices original aes key is " + decryptedKey );
        
        byte[] raw = key.getEncoded();
		FileOutputStream fos3 = new FileOutputStream("backtoAES.txt"); //wrote the aes key to a file
		fos3.write(raw);
		fos3.close();
		Encryption eobj = new Encryption();
		
		
		decrypt("clear.txt.aes",key ); // now that bob decrypted alices encrpted rsa public key he should now be able to decrypt the data.
		
		//send the ENCRYPTED DATA file that was encrypted by alice along with the key bob just discovered with his rsa private
		
        return key; // key is the decrypted RSA key that alice originally encrypted with bobs rsa public
    }
	
	
	public void decrypt(String fname, SecretKey key)throws Exception{
	     
		  //creating file input stream to read from file
		  try(FileInputStream fis = new FileInputStream(fname)){
		   //creating object input stream to read objects from file
		   
	        String keyused = Base64.getEncoder().encodeToString(key.getEncoded());
	        
		   
		   System.out.println("decrypting " + fname + " using " + keyused);
		   
			  
		  
		   Cipher aesCipher = Cipher.getInstance("AES");  //getting cipher for AES
		   aesCipher.init(Cipher.DECRYPT_MODE, key);  //initializing cipher for decryption with decrypted aes key
		   //creating file output stream to write back original contents
		   try(FileOutputStream fos = new FileOutputStream(fname +".dec"))
		   {
		    //creating cipher input stream to read encrypted contents
			   try(CipherInputStream cis = new CipherInputStream(fis, aesCipher))
			   {
				   int read;
				   byte buf[] = new byte[4096];
				   while((read = cis.read(buf)) != -1)
				   {  //reading from file
					   fos.write(buf, 0, read);  //decrypting and writing to file
				   }
				   
				   cis.close();
		     
			   }
		    
		   }
		   System.out.println("decryption complete look in file clear.txt.dec to see if the data inside matches the original clear.txt file");
		   fis.close();
		  }
		  
		  
		  
		 }

	/**
	 * This method is used in the very beginning when running the Encryption Class 
	 * Bob must first verify the mac sent by alice before being able to continue
	 * with any decryption of data.
	 * Bob uses the symettric key provided by Alice to Apply a mac to the same 
	 * ciphertext as alice did...if the mac in the end (dataaftermac) is 
	 * equal to the mac alice sent(alicesmac) then the verification was successful
	 * 
	 * @param macdata data to apply the mac to
	 * @param hmackey symmetric key used to generate the mac
	 * @param alicesmac the mac generated by alice
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 */

	public void verifyMAC(byte[] macdata, Key hmackey, byte[] alicesmac) throws IOException, NoSuchAlgorithmException, InvalidKeyException {
		
		byte[] thiskey = Files.readAllBytes(new File("mackey.txt").toPath()); // bob must first get the key used to generate the mac from alice
		
		if(thiskey.length > 0){
			System.out.println("Bob: reading the mackey from mackey.txt was successful");
			
		}
		else{
			System.out.println("Bob: bob could not read the mackey from alice");
			
		}
		
		byte[] data = Files.readAllBytes(new File("clear.txt.aes").toPath());
		
		
		Mac mac = Mac.getInstance("HmacSHA1");
		mac.init(hmackey);
		byte [] dataaftermac = mac.doFinal(data);
		
		
		
		String dataAfterMac = Base64.getEncoder().encodeToString(dataaftermac);
		System.out.println("Bob: this data should be equal to alice's data if verification is correct");
		System.out.println("Bob: " + dataAfterMac);

		if(Arrays.equals(dataaftermac, alicesmac)){
			
			System.out.println("Bob: Mac was successfully verified!! Encryption can now continue");
			
		}
		else{
			System.out.println("Mac was not verified ending all communication");
			System.exit(0);
		}
		
		
		
	}
	
	
	
	

}