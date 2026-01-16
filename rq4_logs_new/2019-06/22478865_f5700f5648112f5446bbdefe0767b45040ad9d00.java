package gov.nasa.jpl.labcas.data_access_api.jwt;

import com.auth0.jwt.algorithms.Algorithm;

public class Constants {
	
	public static Algorithm algorithm = Algorithm.HMAC256("secret");
	
	public static String ISSUER = "LabCAS";
	
	public static String CLAIM_FILENAME = "FileName";
	
	public static int EXPIRES_IN_SECONDS = 60;

}