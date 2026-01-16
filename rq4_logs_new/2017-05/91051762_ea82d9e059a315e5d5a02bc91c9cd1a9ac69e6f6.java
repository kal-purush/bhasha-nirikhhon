/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.IOException;

import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.amazonaws.services.simpleemail.model.SendEmailResult;
import com.amazonaws.services.simpleemail.model.VerifyEmailAddressRequest;

public class AmazonSESSample {

    static final String FROM = "Ecom SOS <cskh@fptshop.com.vn>";  // Replace with your "From" address. This address must be verified.
    static final String TO = "hungnguyennghia@vccorp.vn";//"hungnn2504@gmail.com"; // Replace with a "To" address. If you have not yet requested
                                                      // production access, this address must be verified.
    static final String BODY = "This email was sent through Amazon SES by using the AWS SDK for Java.";
    static final String SUBJECT = "Amazon SES test (AWS SDK for Java)";

    /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (C:\\Users\\PC0353\\.aws\\credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    public static boolean isValidEmailAddress(String email) {
 	   boolean result = true;
 	   try {
 	      InternetAddress emailAddr = new InternetAddress(email);
 	      emailAddr.validate();
 	   } catch (AddressException ex) {
 	      result = false;
 	   }
 	   return result;
 	}

    
//    public static void main(String[] args) throws IOException {
//    	Jedis jedis = new Jedis("localhost", 6379);
//    //	JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
//    	
//    	//JedisPool pool = new JedisPool("localhost", 6379);  
//    	  
//    //	JedisPool pool = new JedisPool("localhost");
//    //	Jedis jedis = pool.getResource();  
//    	
//    	// clear the redis list first
//    	
//    	System.out.println(jedis.get("hungnn2504@gmail.com1"));
//    }
    
    public static void main(String[] args) throws IOException {

        // Construct an object to contain the recipient address.
    	String cc="";
        Destination destination = new Destination().withToAddresses(TO.split(","));
        if(cc.contains("@"))destination.withCcAddresses(cc.split(","));
        // Create the subject and body of the message.
        Content subject = new Content().withData(SUBJECT);
        Content textBody = new Content().withData(BODY);
        Body body = new Body().withHtml(textBody);

        // Create a message with the specified subject and body.
        Message message = new Message().withSubject(subject).withBody(body);

        // Assemble the email.
        SendEmailRequest request = new SendEmailRequest().withSource(FROM).withDestination(destination).withMessage(message);

        try {
            System.out.println("Attempting to send an email through Amazon SES by using the AWS SDK for Java...");

            /*
             * The ProfileCredentialsProvider will return your [default]
             * credential profile by reading from the credentials file located at
             * (C:\\Users\\PC0353\\.aws\\credentials).
             *
             * TransferManager manages a pool of threads, so we create a
             * single instance and share it throughout our application.
             */
            
		     String keyID = "AKIAIPBRNZKKVCX4VKQA";//<your key id>
		     String secretKey = "REegaW1RapoBxBS8BbVp6+9JzOhIrOJIjtR67b2J";//<your secret key>
		     
		 BasicAWSCredentials credentials = new BasicAWSCredentials(keyID,secretKey);
//            AWSCredentials credentials = null;
//            try {
//                credentials = new ProfileCredentialsProvider("default").getCredentials();
//            } catch (Exception e) {
//                throw new AmazonClientException(
//                        "Cannot load the credentials from the credential profiles file. " +
//                        "Please make sure that your credentials file is at the correct " +
//                        "location (C:\\Users\\PC0353\\.aws\\credentials), and is in valid format.",
//                        e);
//            }

//		 ClientConfiguration clientConfig = new ClientConfiguration();
//		 clientConfig.setProtocol(Protocol.HTTPS);
//		 clientConfig.setProxyHost("10.1.7.55");
//		 clientConfig.setProxyPort(8080);

            // Instantiate an Amazon SES client, which will make the service call with the supplied AWS credentials.
            AmazonSimpleEmailServiceClient client = new AmazonSimpleEmailServiceClient(credentials);

            
            // Choose the AWS region of the Amazon SES endpoint you want to connect to. Note that your production
            // access status, sending limits, and Amazon SES identity-related settings are specific to a given
            // AWS region, so be sure to select an AWS region in which you set up Amazon SES. Here, we are using
            // the US East (N. Virginia) region. Examples of other regions that Amazon SES supports are US_WEST_2
            // and EU_WEST_1. For a complete list, see http://docs.aws.amazon.com/ses/latest/DeveloperGuide/regions.html
            Region REGION = Region.getRegion(Regions.US_EAST_1);
            client.setRegion(REGION);

            
//            AmazonSimpleEmailService client1 = new AmazonSimpleEmailServiceClient();
//            VerifyEmailAddressRequest request1 = new VerifyEmailAddressRequest()
//                    .withEmailAddress("user@example.com");
//            VerifyEmailAddressResult response = client.verifyEmailAddress(request1);
//            response.toString();
//            response.getSdkResponseMetadata().
            // Send the email.
//            client.sendEmail(request);
            SendEmailResult result = client.sendEmail(request);
            
            System.out.println("messageId:"+result.getMessageId());
            System.out.println("Email sent!");
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS CloudFormation, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException e) {
        	System.out.println(e.getLocalizedMessage());
        	System.out.println(e.getMessage());
        	String err=e.getMessage();
            String des=err.substring(0,err.indexOf("("));
            String cterr=err.substring(err.indexOf("(")+1,err.indexOf(")"));
            String[] errContent=cterr.split(";");
            for (int i = 0; i < errContent.length; i++) {
				System.out.println(errContent[i]);
			}
            System.out.println("mota:"+des);
        	
        } catch (Exception ex) {
            System.out.println("The email was not sent.");
            System.out.println("Error message: " + ex.getMessage());            
            
        }
    }
}