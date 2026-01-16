package it.gov.pagopa.pu.classification.service;

import it.veneto.regione.schemas._2012.pagamenti.ente.CtBilancio;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;

@Lazy
@Component
@Slf4j
public class BalanceUnmashallerService {
  private final JAXBContext jaxbContext;
  private final Schema schema;
  private final XMLUnmarshallerService xmlUnmarshallerService;


  public BalanceUnmashallerService(@Value("classpath:xsd/PagInf_Dovuti_Pagati_6_2_0.xsd") Resource paymetsReportingXsdResource,
                                   XMLUnmarshallerService xmlUnmarshallerService) {
    try {
      this.jaxbContext = JAXBContext.newInstance(CtBilancio.class);
      SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      this.schema = schemaFactory.newSchema(paymetsReportingXsdResource.getURL());
      this.xmlUnmarshallerService = xmlUnmarshallerService;
    } catch (JAXBException | SAXException | IOException e) {
      throw new IllegalStateException("Error while creating jaxb context for CtBilancio", e);
    }
  }

  /**
   * Unmarshals a file into a CtFlussoRiversamento object.
   *
   * @param xmlString the XML string to parse
   * @return the unmarshalled CtFlussoRiversamento object
   */
  public CtBilancio unmarshal(String xmlString) {
    return xmlUnmarshallerService.unmarshal(xmlString, CtBilancio.class, jaxbContext, schema);
  }

}