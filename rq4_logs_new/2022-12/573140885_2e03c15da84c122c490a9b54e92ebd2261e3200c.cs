using System.Xml;

using Handlers;

using Models;

using Services;

namespace Jobs;

public class Producer
{
    private Partner partner;
    private XmlDSigService signer;

    public Producer(Partner _partner)
    {
        partner = _partner;
        signer = new XmlDSigService(partner.Secrets);
    }

    public async Task<List<(XmlDocument, string)>> GetPayloadToProcess()
    {
        var results = new List<(XmlDocument, string)>();

        string filepath = Path.Join(".", "Db", "data.csv");
        List<Customer> customers = CSVHandler.Read<Customer>(filepath);

        foreach (var customer in customers)
        {
            var result = await GetPayload(customer);

            results.Add(result);
        }

        return results;
    }

    public async Task<(XmlDocument, string)> GetPayload(Customer customer)
    {
        InvoiceRequest request = await MountPayload(customer);
        XmlDocument payload = signer.SignInvoice(request);

        return (payload, customer.Id);
    }

    private async Task<InvoiceRequest> MountPayload(Customer customer)
    {
        List<InvoiceRequest.ItemServico> itensServico = InvoiceService.GetItensServico(partner, customer);
        var (baseCalculo, valorIssqn, valorTotalServicos) = InvoiceService.GetTotals(itensServico);

        Address address = await AddressService.Get(customer.PostalCode);

        InvoiceRequest result = new()
        {
            // Identificacao = "#0001", // TODO: string 10 chars
            NumeroAedf = partner.Municipal.NumeroAedf,
            DataEmissao = UtilsHandler.MaskDate(customer.EffectiveDate),
            //
            ItensServico = itensServico,
            //
            BaseCalculo = baseCalculo,
            ValorIssqn = valorIssqn,
            ValorTotalServicos = valorTotalServicos,
            // DadosAdicionais = MountAdditionalData(), // TODO
            //
            RazaoSocialTomador = customer.FullName,
            EmailTomador = customer.Email,
            IdentificacaoTomador = UtilsHandler.MaskPersonDocument(customer.DocumentId),
            //
            PaisTomador = address.CountryCode,
            CodigoMunicipioTomador = address.MunicipalCode,
            LogradouroTomador = address.Street,
            NumeroEnderecoTomador = "s/n",
            BairroTomador = address.Neighborhood,
            CodigoPostalTomador = address.PostalCode,
            UfTomador = address.State,
            Cfps = CityCodesService.GetCfps(address.MunicipalCode),
        };

        return result;
    }

    private string MountAdditionalData()
    {
        string additionalData = $"Valor Aprox. dos Trib. de acordo Lei 12.741/12 Federal R$ 171,20 (13,45%) - Estadual R$ 0 0,00 - Municipal R$ 48,75 (3.83%)";

        return String.Empty;
    }
}