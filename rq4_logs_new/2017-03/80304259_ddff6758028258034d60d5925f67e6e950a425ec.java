package manager;

import exception.user.UserNotFoundException;
import model.DHParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.DHParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.HashMap;
import java.util.Map;

public class EncryptionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(EncryptionManager.class);

  private static final int RANDOM_NUMBER_BIT_LENGTH = 500;
  private static final String ENCRYPTION_MODE = "AES/CBC/PKCS5Padding";
  private static final String ENCRYPTION_ALGORITHM = "AES";

  Map<String, byte[]> sharedKeys;

  private Map<String, BigInteger> privateExponents;
  private AlgorithmParameterSpec parameterSpec;
  private DHParameterSpec params;
  private Cipher cipher;

  private static EncryptionManager instance;

  EncryptionManager() throws NoSuchAlgorithmException, NoSuchPaddingException {
    sharedKeys = new HashMap<>();
    privateExponents = new HashMap<>();

    BigInteger g = BigInteger.valueOf(5);
    BigInteger p = BigInteger.probablePrime(RANDOM_NUMBER_BIT_LENGTH, new SecureRandom());
    params = new DHParameterSpec(p, g);
    byte[] IV = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
    parameterSpec = new IvParameterSpec(IV);
    cipher = Cipher.getInstance(ENCRYPTION_MODE);
  }

  public static EncryptionManager getInstance() {
    if (instance == null)
      try {
        instance = new EncryptionManager();
      }
      catch (GeneralSecurityException e){
        LOGGER.error("Something went wrong when creating the encryption manager");
      }
    return instance;
  }

  /**
   * Generates initial Diffie-Hellman parameters to the user.
   * Also creates the public key that will be associated with the user.
   *
   * @param username non-null string of the user who is requesting the Diffie-Hellman key.
   * @return public Diffie-Hellman parameters.
   */
  public DHParams getParameters(String username) {
    if (privateExponents.get(username) == null) {
      BigInteger s = BigInteger.probablePrime(RANDOM_NUMBER_BIT_LENGTH, new SecureRandom());
      privateExponents.put(username, s);
    }
    BigInteger publicParam = modExp(params.getG(), privateExponents.get(username), params.getP());
    LOGGER.info("Generating public parameter {] to {]", publicParam, username);
    return new DHParams(params, publicParam.toByteArray());
  }

  /**
   * Creates shared key based on the public key given in the body.
   *
   * @param username non-null string of the user who is requesting Diffie-Hellman.
   * @param parameters Diffie-Hellman parameters of the user.
   * @throws UserNotFoundException if user hasn't requested the initial public parameters.
   */
  public void setUpSharedKey(@PathVariable String username, @RequestBody DHParams parameters) throws UserNotFoundException {
    LOGGER.info("Setting up shared key for {} with parameters {}", username, parameters);
    BigInteger publicParam = new BigInteger(parameters.getPublicParam());
    if (privateExponents.get(username) == null) {
      throw new UserNotFoundException("Request initial parameters first");
    }
    BigInteger sharedKey = modExp(publicParam, privateExponents.get(username), parameters.getParams().getP());
    byte[] result = new byte[16];
    for (int i = 0; i < 16; i++) {
      result[i] = sharedKey.toByteArray()[i];
    }
    sharedKeys.put(username, result);
  }

  /**
   * Decrypts password based on the shared key set up with the user earlier.
   *
   * @param username non-null string of the user who's password is being decrypted.
   * @param password non-null string containing the encrypted password.
   * @return decrypted password
   * @throws GeneralSecurityException if algorithm does not exist.
   */
  public String decryptPassword(String username, String password) throws GeneralSecurityException {
    SecretKeySpec key = new SecretKeySpec(sharedKeys.get(username), ENCRYPTION_ALGORITHM);
    cipher.init(Cipher.DECRYPT_MODE, key, parameterSpec);
    return new String(cipher.doFinal(password.getBytes()));
  }

  BigInteger modExp(BigInteger x, BigInteger y, BigInteger n) {
    if (y.equals(BigInteger.valueOf(0))) {
      return BigInteger.valueOf(1);
    }
    BigInteger z = modExp(x, y.divide(BigInteger.valueOf(2)), n);
    if (y.mod(BigInteger.valueOf(2)).equals(BigInteger.valueOf(0))) {
      return z.multiply(z).mod(n);
    }
    else {
      return x.multiply((z.multiply(z)).mod(n)).mod(n);
    }
  }
}