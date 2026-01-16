package br.gov.caixa.sibar.ws.client;

import java.io.StringWriter;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import br.gov.caixa.sibar.HEADERBARRAMENTOTYPE;
import br.gov.caixa.sibar.consulta_saldo.DadosConsultaSaldoEType;
import br.gov.caixa.sibar.consulta_saldo.DadosContaType;
import br.gov.caixa.sibar.consulta_saldo.DadosEntradaType;
import br.gov.caixa.sibar.consulta_saldo.ServicoEntradaNegocialType;
import br.gov.caixa.sibar.consulta_saldo.ServicoSaidaNegocialType;
import br.gov.caixa.sibar.consulta_saldo.ws.ConsultaSaldo;
import br.gov.caixa.sibar.consulta_saldo.ws.ConsultaSaldo_Service;

public class TesteConsultaSaldoSOAPClient {
	
	private static TrustManager[] get_trust_mgr() {
		TrustManager[] certs = new TrustManager[] { new X509TrustManager() {
			public X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			public void checkClientTrusted(X509Certificate[] certs, String t) {
			}

			public void checkServerTrusted(X509Certificate[] certs, String t) {
			}
		} };
		return certs;
	}
	
	private static void initByPass(){
		try {
			SSLContext ssl_ctx = SSLContext.getInstance("TLS");
			TrustManager[] trust_mgr = get_trust_mgr();
			ssl_ctx.init(null, // key manager
					trust_mgr, // trust manager
					new SecureRandom()); // random number generator
			HttpsURLConnection.setDefaultSSLSocketFactory(ssl_ctx.getSocketFactory());
		} catch (KeyManagementException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws JAXBException {
		System.out.println("--- Inicio Aplicacao ---");
		initByPass();
		/* --- Preparecao para a Requisição --- */
		ConsultaSaldo_Service service = new ConsultaSaldo_Service();
		ConsultaSaldo server = service.getConsultaSaldoSOAP();
		
		/* --- Instanciacao dos objetos que representam as Tags dos XMLs --- */
		ServicoEntradaNegocialType entradaNegocialType = new ServicoEntradaNegocialType();
		HEADERBARRAMENTOTYPE headerTag = new HEADERBARRAMENTOTYPE();
		DadosEntradaType dadosTag = new DadosEntradaType();
		DadosConsultaSaldoEType consultaSaldoTag = new DadosConsultaSaldoEType();
		DadosContaType contaTag = new DadosContaType();
		
		/* --- Preenchimento do objeto que representa a TAG CONTA --- */
		contaTag.setAGENCIA((short)239);
		contaTag.setOPERACAO((short)001);
		contaTag.setNUMERO(41);
		contaTag.setDV((short)6);
		
		/* --- Colocando a TAG CONTA dentro da TAG CONSULTA_SALDO --- */
		consultaSaldoTag.setCONTA(contaTag);
		
		/* --- Preenchimento da TAG do HEADER do BARRAMENTO --- */
		headerTag.setVERSAO("2.0");
		headerTag.setUSUARIOSERVICO("SMTXSD01");
		headerTag.setOPERACAO("CONSULTA_SALDO");
		headerTag.setSISTEMAORIGEM("SIMTX");
		headerTag.setDATAHORA(new SimpleDateFormat("yyyyMMddhhmmss").format(new Date()));
		
		/* --- Colocando a TAG CONSULTA_SALDO dentro da TAG DADOS --- */
		dadosTag.setCONSULTASALDO(consultaSaldoTag);
		
		/* --- Set do Header e dos Dados Negociais --- */
		entradaNegocialType.setHEADER(headerTag);
		entradaNegocialType.setDADOS(dadosTag);		
		
		/* --- Envio e Recebimento da Resposta --- */
		ServicoSaidaNegocialType dadosSaida = server.consultaSALDO(entradaNegocialType);
		
		/* --- Conversao da Resposta para XML e impressao de Sysout --- */
		JAXBContext jaxbContext = JAXBContext.newInstance(ServicoSaidaNegocialType.class);
		Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
		jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		StringWriter writer = new StringWriter();
		System.out.println(dadosSaida);
		jaxbMarshaller.marshal(dadosSaida, writer);
		
		System.out.println(writer);
		System.out.println("--- Fim Aplicacao ---");
	}
}