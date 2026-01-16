using System.Xml;

using Models;
using Services;
using Handlers;

namespace Jobs;

public static class Consumer
{
    public static async Task<InvoiceResponse?> Run(Producer producer, InvoiceRequest request)
    {
        Console.WriteLine($"Processing payload {request.Identificacao}");

        try
        {
            XmlDocument xmlInvoice = XmlHandler.SerializeObjectToXmlDocument(request);
            XmlDocument signedInvoice = XmlDSigService.XmlDSig(xmlInvoice, producer.certificate!);

            OAuth2TokenService? refreshToken = await InvoiceService.RefreshToken(producer.credentials!);
            if (refreshToken is null || refreshToken.IsNotValidToken())
            {
                return null;
            }

            var (response, statusCode) = await InvoiceService.Create(signedInvoice, refreshToken);
            XmlDocument xmlResponse = new() { PreserveWhitespace = true };
            xmlResponse.Load(response);

            InvoiceResponse result = XmlHandler.DeserializeResponse<InvoiceResponse>(xmlResponse);

            return result;
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
            return null;
        }
    }
}