package org.wso2.mb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.Properties;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 *
 */
public class BrokerJMXClient {

    public static void main(String[] args) throws MalformedObjectNameException, IOException, MBeanException,
            InstanceNotFoundException, ReflectionException, IntrospectionException, AttributeNotFoundException {
        new BrokerJMXClient().invoke();

    }

    public void invoke() throws IOException, MalformedObjectNameException, MBeanException, InstanceNotFoundException,
            ReflectionException, IntrospectionException, AttributeNotFoundException {
        Properties props = loadProperties();

        JMXServiceURL url = new JMXServiceURL(props.getProperty("jmx.service.url"));

        Hashtable<String, String[]> env = new Hashtable<>();
        String[] credentials = new String[]{props.getProperty("username"), props.getProperty("password")};
        env.put(JMXConnector.CREDENTIALS, credentials);

        JMXConnector jmxConnector = JMXConnectorFactory.connect(url, env);
        MBeanServerConnection mbeanServerConnection = jmxConnector.getMBeanServerConnection();

        ObjectName mbeanObjectName = new ObjectName(props.getProperty("mbean.name"));

        String[] signature = getSignatureArray(props);
        Object[] arguments = getArgumentsArray(props);

        Object result;
        try {
            if (props.stringPropertyNames().contains("mbean.operation.name")) {
                result = mbeanServerConnection.invoke(
                        mbeanObjectName,
                        props.getProperty("mbean.operation.name"),
                        arguments,
                        signature);
            } else {
                result = mbeanServerConnection.getAttribute(
                        mbeanObjectName,
                        props.getProperty("mbean.attribute.name"));
            }
        } catch (ReflectionException | AttributeNotFoundException e) {
            MBeanInfo mBeanInfo = mbeanServerConnection.getMBeanInfo(mbeanObjectName);
            System.out.println("Available Attributes : ");
            for (MBeanAttributeInfo mBeanAttributeInfo : mBeanInfo.getAttributes()) {
                System.out.println("\t" + mBeanAttributeInfo.getName());
            }

            System.out.println("Available Operations : ");
            for (MBeanOperationInfo mBeanOperationInfo : mBeanInfo.getOperations()) {
                System.out.println("\t" + mBeanOperationInfo.getName());
            }

            throw e;
        }

        try {
            System.out.println(result.toString());
        } catch (Exception e) {
            System.out.println("Error while printing toString() of the return result set.");
            e.printStackTrace();
        }

        jmxConnector.close();
    }

    public Properties loadProperties() throws IOException {
        Properties prop = new Properties();
        String propFileName = "jmx-config.properties";

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            prop.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }

        return prop;
    }

    public String[] getSignatureArray(Properties props) {
        String signature = props.getProperty("signature");

        if (signature.isEmpty()) {
            return null;
        }

        String[] signatures = signature.split(",");
        String[] signatureTrimmedArray = new String[signatures.length];
        for (int i = 0; i < signatures.length; i++) {
            signatureTrimmedArray[i] = signatures[i].trim();
        }

        return signatureTrimmedArray;
    }

    private Object[] getArgumentsArray(Properties props) {
        String arguments = props.getProperty("arguments");

        if (arguments.isEmpty()) {
            return null;
        }

        String[] argumentsArray = arguments.split(",");
        String[] signatureArray = getSignatureArray(props);
        Object[] castedArguments = new Object[signatureArray.length];
        for (int i = 0; i < signatureArray.length; i++) {
            if (Boolean.class.getName().equals(signatureArray[i])) {
                castedArguments[i] = Boolean.parseBoolean(argumentsArray[i].trim());
            } else if (Byte.class.getName().equals(signatureArray[i])) {
                castedArguments[i] = Byte.parseByte(argumentsArray[i].trim());
            } else if (Short.class.getName().equals(signatureArray[i])) {
                castedArguments[i] = Short.parseShort(argumentsArray[i].trim());
            } else if (Integer.class.getName().equals(signatureArray[i])) {
                castedArguments[i] = Integer.parseInt(argumentsArray[i].trim());
            } else if (Long.class.getName().equals(signatureArray[i])) {
                castedArguments[i] = Long.parseLong(argumentsArray[i].trim());
            } else if (Float.class.getName().equals(signatureArray[i])) {
                castedArguments[i] = Float.parseFloat(argumentsArray[i].trim());
            } else if (Double.class.getName().equals(signatureArray[i])) {
                castedArguments[i] = Double.parseDouble(argumentsArray[i].trim());
            } else {
                castedArguments[i] = argumentsArray[i];
            }
        }

        return castedArguments;
    }
}