#nullable disable

using System.Xml;
using System.Xml.Serialization;

namespace Model;

[XmlRoot("xmlCancelamentoNfpse")]
public class InvoiceCancelRequest
{
    [XmlElement(ElementName = "motivoCancelamento")]
    public string MotivoCancelamento { get; set; }

    [XmlElement(ElementName = "nuAedf")]
    public string NuAedf { get; set; }

    [XmlElement(ElementName = "nuNotaFiscal")]
    public long NuNotaFiscal { get; set; }

    [XmlElement(ElementName = "codigoVerificacao")]
    public string? CodigoVerificao { get; set; }
}