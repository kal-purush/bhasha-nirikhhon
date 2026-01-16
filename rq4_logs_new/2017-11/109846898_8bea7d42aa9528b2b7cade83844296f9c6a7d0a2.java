package aws.iot;

import com.amazonaws.services.iot.client.AWSIotException;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.AWSIotTimeoutException;
import com.amazonaws.services.iot.client.sample.sampleUtil.SampleUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class publisher {
  private static final AWSIotQos QOS = AWSIotQos.QOS0;
  private static final String TOPIC = "topic/MyIoTButtonTopìc";
  private static final String PAYLOAD = "{\n" +
      "    \"deviceid\" : \"iot123\",\n" +
      "    \"temp\" : 54.98,\n" +
      "    \"humidity\" : 32.43,\n" +
      "    \"coords\" : {\n" +
      "        \"latitude\" : 47.615694,\n" +
      "        \"longitude\" : -122.3359976\n" +
      "    }\n" +
      "}";
  private static final long QUIESCE_TIMEOUT = 5000;

  public static void main (String args[]) {

    if(args.length < 1) { showHelp(); }

    Properties prop = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream input = loader.getResourceAsStream(args[0]);

    try {
      prop.load(input);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // SampleUtil.java and its dependency PrivateKeyReader.java can be copied from the sample source code.
    // Alternatively, you could load key store directly from a file - see the example included in this README.
    SampleUtil.KeyStorePasswordPair pair = SampleUtil.getKeyStorePasswordPair(prop.getProperty("certificateFile"), prop.getProperty( "privateKeyFile"));
    AWSIotMqttClient client = new AWSIotMqttClient(prop.getProperty("clientEndpoint"), prop.getProperty("clientId"), pair.keyStore, pair.keyPassword);

    // optional parameters can be set before connect()
    try {
      client.connect();

      client.publish(TOPIC, QOS, PAYLOAD);

      client.disconnect(QUIESCE_TIMEOUT);

    } catch (AWSIotException e) {
      e.printStackTrace();
    } catch (AWSIotTimeoutException e) {
      e.printStackTrace();
    }
  }

  private static void showHelp()  {
    System.out.println("Usage: java -classpath targe/aws-1.0-SNAPSHOT.jar aws.iot.publisher <config-file>\n");
    System.out.println("See resources/config.properties for an example of a config file.\n");
    System.exit(0);
  }
}