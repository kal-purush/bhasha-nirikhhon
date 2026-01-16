using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace collectorShop.lib
{
    public class ConsultaCorreios
    {
        private string cdEmpresa;
        private string senhaEmpresa;
        private CodServico codServico;
        private string cepOrigem;
        private string cepDestino;
        private string pesoEncomenda;
        private FormatoEncomenda formatoEncomenda;
        private decimal comprimento;
        private decimal altura;
        private decimal largura;
        private decimal diametro;
        private string maoPropria;
        private decimal valorDeclarado;
        private string avisoRecebimento;

        public string CdEmpresa { get => cdEmpresa; set => cdEmpresa = value; }
        public string SenhaEmpresa { get => senhaEmpresa; set => senhaEmpresa = value; }
        public CodServico CodServico1 { get => codServico; set => codServico = value; }
        public string CepOrigem { get => cepOrigem; set => cepOrigem = value; }
        public string CepDestino { get => cepDestino; set => cepDestino = value; }
        public string PesoEncomenda { get => pesoEncomenda; set => pesoEncomenda = value; }
        public FormatoEncomenda FormatoEncomenda1 { get => formatoEncomenda; set => formatoEncomenda = value; }
        public decimal Comprimento { get => comprimento; set => comprimento = value; }
        public decimal Altura { get => altura; set => altura = value; }
        public decimal Largura { get => largura; set => largura = value; }
        public decimal Diametro { get => diametro; set => diametro = value; }
        public string MaoPropria { get => maoPropria; set => maoPropria = value; }
        public decimal ValorDeclarado { get => valorDeclarado; set => valorDeclarado = value; }
        public string AvisoRecebimento { get => avisoRecebimento; set => avisoRecebimento = value; }

        public string ValorServico()
        {
            ServiceReference1.CalcPrecoPrazoWSSoapClient webServiceCorreios = new ServiceReference1.CalcPrecoPrazoWSSoapClient();


            // Efetuo a requisição
            ServiceReference1.cResultado retornoCorreios = webServiceCorreios.CalcPrecoPrazoAsync(cdEmpresa, SenhaEmpresa, Convert.ToInt32(codServico).ToString, cepOrigem, 
                CepDestino, pesoEncomenda, Convert.ToInt32(formatoEncomenda), comprimento, altura, largura, diametro, maoPropria, valorDeclarado, avisoRecebimento) ;

            // Verifico se há retorno
            if (retornoCorreios.Servicos.Length > 0)
            {
                // Se deu tudo certo, então retorna o valor
                if (retornoCorreios.Servicos[0].Erro == "0")
                    return "R$ " + retornoCorreios.Servicos[0].Valor;
                else
                    return retornoCorreios.Servicos[0].MsgErro;
            }
            else
                return "NÃO FOI POSSÍVEL CONSULTAR O SERVIÇO DESEJADO!";
        }
        public enum CodServico
        {
            PacSemContrato = 41105,
            SedexSemContrato = 40010,
            SedexCobrarSemContrato = 40045,
            SedexCobrarComContrato = 40126,
            SedexHojeSemContrato = 40290,
            SedexComContrato = 40096
        }
        public enum FormatoEncomenda
        {
            CaixaPacote = 1,
            RoloPrisma = 2
        }

    }
}