package ch.vivates.tools.sec;

import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.opensaml.DefaultBootstrap;
import org.opensaml.common.SAMLException;
import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.Conditions;
import org.opensaml.security.SAMLSignatureProfileValidator;
import org.opensaml.xml.Configuration;
import org.opensaml.xml.ConfigurationException;
import org.opensaml.xml.io.Unmarshaller;
import org.opensaml.xml.io.UnmarshallerFactory;
import org.opensaml.xml.io.UnmarshallingException;
import org.opensaml.xml.security.credential.Credential;
import org.opensaml.xml.signature.Signature;
import org.opensaml.xml.signature.SignatureValidator;
import org.opensaml.xml.validation.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.w3c.dom.Element;

/**
 * The Class SamlHelper provides the needed functionality which involves the OpenSAML Library.
 * 
 * @author Claudia Schwenk, Post CH, major development
 * @author Kevin Tippenhauer, Berner Fachhochschule, javadoc
 */
public class SamlHelper {

	/** The Constant LOG. */
	private static final Logger	LOG	= LoggerFactory.getLogger(SamlHelper.class);

	/** The grace period max value. */
	private final long GRACE_PERIOD_MAX_VALUE = 10000l;

	/** The grace period. */
	private final long gracePeriod;
	
	/** The verify conditions. */
	private final boolean verifyConditions;
	
	/** The verify signature. */
	private final boolean verifySignature;
	
	/** The certificate store. */
	private final CertificateStore certificateStore;
	
	/** The unmarshaller factory. */
	private final UnmarshallerFactory unmarshallerFactory;

	/**
	 * Instantiates a new saml helper.
	 *
	 * @param keystorepath the keystorepath
	 * @param keystorepass the keystorepass
	 * @param keystoreAliases the keystore aliases
	 * @param gracePeriod the grace period
	 * @param verifySignature the verify signature
	 * @param verifyConditions the verify conditions
	 * @throws SAMLException the SAML exception
	 */
	public SamlHelper(final String keystorepath, final String keystorepass, final String keystoreAliases, final long gracePeriod,
			final boolean verifySignature, final boolean verifyConditions) throws SAMLException {
		try {
			LOG.info("Initialize OpenSAML");
			DefaultBootstrap.bootstrap();
		} catch (ConfigurationException e) {
			throw new SAMLException("OpenSAML could not be initialized.", e);
		}

		if (StringUtils.isBlank(keystorepath) || StringUtils.isBlank(keystorepass)) {
			throw new BeanInitializationException("Missing keystore information.");
		}
		this.certificateStore = new CertificateStore(keystorepath, keystorepass, keystoreAliases);

		this.gracePeriod = Math.min(gracePeriod, GRACE_PERIOD_MAX_VALUE);

		this.verifyConditions = verifyConditions;
		this.verifySignature = verifySignature;

		this.unmarshallerFactory = Configuration.getUnmarshallerFactory();
	}

	/**
	 * Verifies an Assertion and its Signature. Throws an exception when assertion, signature
	 * or username/SPProvidedID is invalid.
	 *
	 * @param assertion the assertion to verify
	 * @throws SAMLException the SAML exception
	 */
	public void verify(Assertion assertion) throws SAMLException {
		LOG.debug("Verify assertion (verifyConditions={}, verifySignature={})", verifyConditions, verifySignature);

		if (verifyConditions) {
			// Verify time
			Conditions conditions = assertion.getConditions();
			if (conditions == null) {
				throw new SAMLException("SAML Assertion is missing Conditions");
			}

			DateTime notBefore = conditions.getNotBefore();
			DateTime notOnOrAfter = conditions.getNotOnOrAfter();

			if (notBefore == null || notOnOrAfter == null) {
				throw new SAMLException("SAML Assertion is missing time conditions");
			}

			// We get the milliseconds for the 2 DateTime(s) we have.
			// http://joda-time.sourceforge.net/apidocs/org/joda/time/base/BaseDateTime.html#getMillis%28%29
			// Gets the milliseconds of the datetime instant from the Java epoch of 1970-01-01T00:00:00Z.
			long notBeforeMillis = notBefore.getMillis();
			long notOnOrAfterMillis = notOnOrAfter.getMillis();

			// Next get the current time in Millis
			// http://download.oracle.com/javase/1.5.0/docs/api/java/lang/System.html#currentTimeMillis%28%29
			// the difference, measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
			long currentMillis = System.currentTimeMillis();

			// OK so now we have ALL dates in the offset from the Java epoch
			// so a simple compare can be done...
			if ((currentMillis < notBeforeMillis - gracePeriod) || (notOnOrAfterMillis < currentMillis)) {
				throw new SAMLException("SAML Assertion is expired");
			}
		}

		// verify the signature
		if (verifySignature) {
			verifySignature(assertion.getSignature());
		}

	}

	/**
	 * Verifies the signature and throws an exception if the signature is invalid.
	 *
	 * @param signature the signature to verify
	 * @throws SAMLException the SAML exception
	 */
	public void verifySignature(Signature signature) throws SAMLException {
		LOG.debug("Verify signature");

		// step #1
		if (signature == null) {
			throw new SAMLException("SAML-Assertion is not signed, its signature is missing.");
		}

		// step #2
		try {
			SAMLSignatureProfileValidator samlSignatureProfileValidator = new SAMLSignatureProfileValidator();

			samlSignatureProfileValidator.validate(signature);
		} catch (ValidationException e) {
			throw new SAMLException("Signature of the SAML-assertion does not conform to the SAML-profile of XML Signature.", e);
		}

		// step #3
		boolean isSignatureValid = false;

		for (Entry<String, Credential> credentialEntry : certificateStore.getCredentials().entrySet()) {
			try {
				SignatureValidator signatureValidator = new SignatureValidator(credentialEntry.getValue());

				signatureValidator.validate(signature);

				isSignatureValid = true;

				break;
			} catch (ValidationException vExcp) {
				// this is an expected behavior, as we need to find out, if there is a certificate, that
				// contains such a public key, that was used to create the signature with.
				if (LOG.isDebugEnabled()) {
					LOG.debug("Validation of signature '" + signature + "' failed using the credential identified by alias '" + credentialEntry.getKey() + "'.");
				}
			}
		}

		if (!isSignatureValid) {
			throw new SAMLException("Signature of the SAML-assertion is not signed with any of the available credentials.");
		}
	}

	/**
	 * Unmarshalls an Element that contains an Assertion to a <code>org.opensaml.saml2.core.Assertion</code>.
	 *
	 * @param assertionElement the assertion element
	 * @return the assertion
	 * @throws SAMLException the SAML exception
	 */
	public Assertion unmarshall(Element assertionElement) throws SAMLException {
		LOG.debug("Unmarshall assertion element");

		Unmarshaller unmarshaller = unmarshallerFactory.getUnmarshaller(assertionElement);
		if (unmarshaller == null) {
			throw new SAMLException("Unable to retrieve unmarshaller by DOM Element");
		}

		try {
			Assertion assertion = (Assertion) unmarshaller.unmarshall(assertionElement);
			return assertion;
		} catch (UnmarshallingException ex) {
			throw new SAMLException("Could not unmarshall SAML Assertion", ex);
		}
	}

}