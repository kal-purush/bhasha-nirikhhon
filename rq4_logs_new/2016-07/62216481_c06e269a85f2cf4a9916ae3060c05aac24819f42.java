
public class BasicJCSCredentials  implements JCSCredentials{
	private String accessKey;
	private String secretKey;
	
	public BasicJCSCredentials(String accessKey, String secretKey){
		if (accessKey == null) {
            throw new IllegalArgumentException("Access key cannot be null.");
        }
        if (secretKey == null) {
            throw new IllegalArgumentException("Secret key cannot be null.");
        }
        this.accessKey = accessKey;
        this.secretKey = secretKey;
	}
	
	@Override
	public String getJCSAccessKey() {
		return this.accessKey;
	}

	@Override
	public String getJCSSecretKey() {
		return this.secretKey;
	}

}