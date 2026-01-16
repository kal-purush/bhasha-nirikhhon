using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Security.Cryptography.Xml;
using System.Xml;
using System.Text;
public class SignXML
{
    public static void Main(String[] args)
    {
        try
        {
            // Create a new CspParameters object to specify
            // a key container.
            CspParameters cspParams = new()
            {
                KeyContainerName = "XML_DSIG_RSA_KEY"
            };
            X509Certificate2 certificate = GetCertificateByHash("7b8e8e9407363622a0e0b1c7c21f3f19eaf69499");
            // Create a new RSA signing key and save it in the container.
            RSACryptoServiceProvider rsaKey = new(cspParams);

            var rsa = certificate.GetRSAPrivateKey();
            // Create a new XML document.
            XmlDocument xmlDoc = new()
            {
                // Load an XML file into the XmlDocument object.
                PreserveWhitespace = true
            };
            xmlDoc.Load("test.xml");

            // Sign the XML document.
            SignXml(xmlDoc, rsa);

            Console.WriteLine("XML file signed.");

            // Save the document.
            xmlDoc.Save("test.xml");
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
    public static X509Certificate2 GetCertificateByHash(string certHash)
    {
        X509Certificate2 result = null;

        using (var certStore = new X509Store(StoreLocation.CurrentUser))
        {
            certStore.Open(OpenFlags.ReadOnly);

            var thumbprint = certHash;//string.Join("", certHash.Select(a => string.Format("{0:X}", a).PadLeft(2, '0')));

            var certs = certStore.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, false);

            if (certs.Count > 0)
            {
                result = certs[0];
            }

            certStore.Close();
        }

        return result;
    }
    // Sign an XML file.
    // This document cannot be verified unless the verifying
    // code has the key with which it was signed.
    public static void SignXml(XmlDocument xmlDoc, RSA rsaKey)
    {
        // Check arguments.
        if (xmlDoc == null)
            throw new ArgumentException(null, nameof(xmlDoc));
        if (rsaKey == null)
            throw new ArgumentException(null, nameof(rsaKey));

        // Create a SignedXml object.
        SignedXml signedXml = new(xmlDoc)
        {

            // Add the key to the SignedXml document.
            SigningKey = rsaKey
        };

        // Create a reference to be signed.
        Reference reference = new()
        {
            Uri = ""
        };

        // Add an enveloped transformation to the reference.
        XmlDsigEnvelopedSignatureTransform env = new();
        reference.AddTransform(env);

        // Add the reference to the SignedXml object.
        signedXml.AddReference(reference);

        // Compute the signature.
        signedXml.ComputeSignature();

        // Get the XML representation of the signature and save
        // it to an XmlElement object.
        XmlElement xmlDigitalSignature = signedXml.GetXml();

        // Append the element to the XML document.
        xmlDoc.DocumentElement.AppendChild(xmlDoc.ImportNode(xmlDigitalSignature, true));
    }
}