package aws.iot;

import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.sample.sampleUtil.SampleUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class IoTMQTTClient {
  private AWSIotMqttClient client;

  public IoTMQTTClient (String filename) {
    Properties prop = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream input = loader.getResourceAsStream(filename);

    try {
      prop.load(input);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // SampleUtil.java and its dependency PrivateKeyReader.java can be copied from the sample source code.
    // Alternatively, you could load key store directly from a file - see the example included in this README.
    SampleUtil.KeyStorePasswordPair pair = SampleUtil.getKeyStorePasswordPair(prop.getProperty("certificateFile"), prop.getProperty("privateKeyFile"));
    this.client = new AWSIotMqttClient(prop.getProperty("clientEndpoint"), prop.getProperty("clientId"), pair.keyStore, pair.keyPassword);

    // optional parameters can be set before connect()
    this.client.setKeepAliveInterval(30000);
  }

  public AWSIotMqttClient getClient() {
    return client;
  }
}