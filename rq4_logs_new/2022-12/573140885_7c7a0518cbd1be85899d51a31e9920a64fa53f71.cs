using System.Xml;
using System.Text.Json;
using System.Security.Cryptography;
using System.Security.Cryptography.Xml;
using System.Security.Cryptography.X509Certificates;

using Model;
using Utils;

namespace Services;

public static class XmlDSigService
{
    public static XmlDocument XmlDSig(XmlDocument payload, X509Certificate2 certificate)
    {
        var result = (XmlDocument)payload.CloneNode(true);

        RSA? privateKey = certificate.GetRSAPrivateKey();

        SignedXml signedXml = new(result) { SigningKey = privateKey };
        signedXml.SignedInfo.SignatureMethod = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256";
        signedXml.SignedInfo.CanonicalizationMethod = "http://www.w3.org/2001/10/xml-exc-c14n#";

        KeyInfo keyInfo = new();
        keyInfo.AddClause(new KeyInfoX509Data(certificate));
        signedXml.KeyInfo = keyInfo;

        Reference reference = new() { Uri = String.Empty };
        reference.AddTransform(new XmlDsigEnvelopedSignatureTransform());
        reference.AddTransform(new XmlDsigExcC14NTransform());

        signedXml.AddReference(reference);
        signedXml.ComputeSignature();

        XmlElement? xmlDigitalSignature = signedXml.GetXml();
        result.DocumentElement!.AppendChild(result.ImportNode(xmlDigitalSignature, true));

        return result;
    }

    public static bool IsValidXmlDSig(XmlDocument xml, X509Certificate2 certificate)
    {
        bool isValid;
        SignedXml signedXml = new(xml);

        var signatureElement = xml.GetElementsByTagName("Signature");
        if (signatureElement.Count != 1)
        {
            return false;
        }

        signedXml.LoadXml((XmlElement)signatureElement[0]!);

        if ((signedXml.SignedInfo.References[0] as Reference)?.Uri != String.Empty)
        {
            return false;
        }

        isValid = signedXml.CheckSignature(certificate.GetRSAPublicKey());

        return isValid;
    }

    public static async Task<Credentials> GetCredentials()
    {
        // TODO: get credentials from secure location

        string templatePath = Path.Join(".");
        string? credentials = await FileHandler.ReadAsync("secrets.json", templatePath);

        if (credentials is not null)
        {
            var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            return JsonSerializer.Deserialize<Credentials>(credentials, options)!;
        }

        return new Credentials();
    }

    public static X509Certificate2 GetCertificate(Credentials credentials)
    {
        byte[] bytes = Convert.FromBase64String(credentials.Certificate.Data);

        return new X509Certificate2(bytes, credentials.Certificate.Password);
    }
}