using System;
using System.Security.Cryptography.X509Certificates;
using System.Security.Cryptography.Xml;
using System.Xml;

namespace RDSignatures
{
    public static class Main
    {
        public static SignedXml SignXmlFileWithCertificate(string xml, X509Certificate2 certRoot, X509Certificate2 certChild, String CanocalizationMethod, String SignatureMethod)
        {
            String msgError = "0";
            try
            {
                msgError = "0.5";
                if (null == xml)
                    throw new ArgumentNullException();

                XmlDocument doc = new XmlDocument();
                doc.PreserveWhitespace = false;
                doc.LoadXml(xml);

                SignedXml signedXml = new SignedXml(doc);
                signedXml.SigningKey = certChild.PrivateKey; 
                signedXml.SignedInfo.CanonicalizationMethod = CanocalizationMethod; // "http://www.w3.org/TR/2001/REC-xml-c14n-20010315#WithComments"; //Exemplo: SignedXml.XmlDsigCanonicalizationWithCommentsUrl
                signedXml.SignedInfo.SignatureMethod = SignatureMethod; // "http://www.w3.org/2000/09/xmldsig#rsa-sha1"; //Exemplo: SignedXml.XmlDsigRSASHA1Url

                Reference reference = new Reference();
                reference.Uri = "";

                XmlDsigEnvelopedSignatureTransform env = new XmlDsigEnvelopedSignatureTransform();
                XmlDsigC14NTransform env2 = new XmlDsigC14NTransform();
                reference.AddTransform(env);
                reference.AddTransform(env2);
                signedXml.AddReference(reference);

                var KeyInfoX509 = new KeyInfoX509Data();
                KeyInfoX509.AddCertificate(certChild);
                KeyInfoX509.AddCertificate(certRoot);

                KeyInfo keyInfo = new KeyInfo();
                keyInfo.AddClause(KeyInfoX509);
                signedXml.KeyInfo = keyInfo;

                signedXml.ComputeSignature();

                return signedXml;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static X509Certificate2 ReturnCertificate(Boolean isRoot, Boolean isAuthentication, String nameCertificate)
        {
            try
            {
                X509Store store = new X509Store(StoreName.My, StoreLocation.CurrentUser);

                if (isRoot)
                    store = new X509Store(StoreName.CertificateAuthority, StoreLocation.CurrentUser);

                store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);

                foreach (X509Certificate2 certificate in store.Certificates)
                {
                    if (isRoot)
                    {
                        if (certificate.Subject == nameCertificate)
                            return certificate;
                    }
                    else
                    {
                        if (isAuthentication)
                        {
                            if ((certificate.SubjectName.Name.Contains(nameCertificate) && certificate.IssuerName.Name.Contains("Autenticação")) || certificate.SubjectName.Name.Contains("[Autenticação] " + nameCertificate))
                            {
                                return certificate;
                            }
                        }
                        else
                        {
                            if ((certificate.SubjectName.Name.Contains(nameCertificate) && certificate.IssuerName.Name.Contains("Assinatura")) || certificate.SubjectName.Name.Contains("[Assinatura Qualificada] " + nameCertificate))
                            {
                                return certificate;
                            }
                        }
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}