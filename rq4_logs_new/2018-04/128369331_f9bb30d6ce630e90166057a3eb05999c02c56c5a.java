package de.hsel.idtt.soaptorest;

import org.apache.cxf.Bus;
import org.apache.cxf.endpoint.Server;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.swagger.Swagger2Feature;

import java.util.Arrays;

import javax.xml.ws.Endpoint;
import org.apache.cxf.jaxws.EndpointImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import de.hsel.idtt.soaptorest.devices.MituyoSensorRest;
import de.hsel.idtt.soaptorest.devices.MituyoSensorSoap;


@SpringBootApplication
public class SoapToRestConverterApplication {

	public static void main(String[] args) {
		SpringApplication.run(SoapToRestConverterApplication.class, args);
	}

	@Autowired
	private Bus bus;

	@Bean
	public Endpoint endpointDeviceMituyo() {
		EndpointImpl endpoint = new EndpointImpl(bus, new MituyoSensorSoap());
		endpoint.publish("/mituyo");
		return endpoint;
	}

	@Bean
	public Server rsServerDevice() {
		JAXRSServerFactoryBean endpoint = new JAXRSServerFactoryBean();
		endpoint.setBus(bus);
		endpoint.setProvider(new JacksonJsonProvider());
		endpoint.setAddress("/device/api/");
		endpoint.setServiceBeans(Arrays.<Object>asList(new MituyoSensorRest()));
		endpoint.setFeatures(Arrays.asList(new Swagger2Feature()));
		return endpoint.create();
	}
}