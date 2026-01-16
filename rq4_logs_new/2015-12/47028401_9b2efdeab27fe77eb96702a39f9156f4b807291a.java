//
// Questo file � stato generato dall'architettura JavaTM per XML Binding (JAXB) Reference Implementation, v2.2.7 
// Vedere <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Qualsiasi modifica a questo file andr� persa durante la ricompilazione dello schema di origine. 
// Generato il: 2015.12.17 alle 10:52:09 AM CET 
//


package com.concretepage.soap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Classe Java per anonymous complex type.
 * 
 * <p>Il seguente frammento di schema specifica il contenuto previsto contenuto in questa classe.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="stockID" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "stockID"
})
@XmlRootElement(name = "getStockRequest")
public class GetStockRequest {

    @XmlElement(required = true)
    protected String stockID;

    /**
     * Recupera il valore della propriet� stockID.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getStockID() {
        return stockID;
    }

    /**
     * Imposta il valore della propriet� stockID.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setStockID(String value) {
        this.stockID = value;
    }

}