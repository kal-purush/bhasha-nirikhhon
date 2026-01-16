import { parse } from 'date-fns';
import {
    AcaoSetorBancario,
    DadosBalancoPatrimonialBancario,
    DadosDemonstrativosDeResultadosBancario,
    IndicadoresFundamentalistasSimplificado,
    Oscilacoes
} from '../dtos/models';
import { GenericMapper } from './generic-mapper';

export class AcaoBancariaMapper extends GenericMapper {
    static DetalhesPapelToAcaoBancaria(webScraper: DetalhesPapel[]): AcaoSetorBancario[] {
        const acoesCompletas: AcaoSetorBancario[] = [];

        webScraper.forEach(element => {
            const novaAcao = new AcaoSetorBancario();

            novaAcao.papel = element['Papel'];
            novaAcao.tipo = element['Tipo'];
            novaAcao.setor = element['Setor'];
            novaAcao.subsetor = element['Subsetor'];
            novaAcao.cotacao = this.converterToFloat(element['Cotação']);
            novaAcao.minUltimasCinquentaDuasSemanas = this.converterToFloat(element['Min 52 sem']);
            novaAcao.maxUltimasCinquentaDuasSemanas = this.converterToFloat(element['Max 52 sem']);
            novaAcao.volumeMedioNegociadoUltimosDoisMeses = this.converterToFloat(element['Vol $ méd (2m)']);
            novaAcao.valorMercado = this.converterToFloat(element['Valor de mercado']);
            novaAcao.valorFirma = this.converterToFloat(element['Valor da firma']);
            novaAcao.numeroAcoes = this.converterToFloat(element['Nro. Ações']);
            let data = parse(element['Últ balanço processado'], 'dd/MM/yyyy', new Date());
            novaAcao.ultimoBalancoProcessado = data.toLocaleDateString('en-GB');

            const oscilacoes = new Oscilacoes();
            oscilacoes.diaPorcentagem = this.converterToFloat(element['Dia']);
            oscilacoes.mesPorcentagem = this.converterToFloat(element['Mês']);
            oscilacoes.trintaDiasPorcentagem = this.converterToFloat(element['30 dias']);
            oscilacoes.dozeMesesPorcentagem = this.converterToFloat(element['12 meses']);

            oscilacoes.anoAnteriorPorcentagem = this.converterToDataAndGetYear(element, -1);
            oscilacoes.doisAnosAntesPorcentagem = this.converterToDataAndGetYear(element, -2);
            oscilacoes.tresAnosAntesPorcentagem = this.converterToDataAndGetYear(element, -3);
            oscilacoes.quatroAnosAntesPorcentagem = this.converterToDataAndGetYear(element, -4);
            novaAcao.oscilacoes = oscilacoes;

            let indicadoresFundamentalistas = new IndicadoresFundamentalistasSimplificado();
            indicadoresFundamentalistas.p_l = this.converterToFloat(element['P/L']);
            indicadoresFundamentalistas.p_vp = this.converterToFloat(element['P/VP']);
            indicadoresFundamentalistas.dividendYieldPorcentagem = this.converterToFloat(element['Div. Yield']);
            indicadoresFundamentalistas.crescimentoReceitaCincoAnosPorcentagem = this.converterToFloat(element['Cres. Rec (5a)']);

            indicadoresFundamentalistas.lpa = this.converterToFloat(element['LPA']);
            indicadoresFundamentalistas.vpa = this.converterToFloat(element['VPA']);
            indicadoresFundamentalistas.margemLiquidaPorcentagem = this.converterToFloat(element['Marg. Líquida']);
            indicadoresFundamentalistas.ebit_AtivoPorcentagem = this.converterToFloat(element['EBIT / Ativo']);
            indicadoresFundamentalistas.roePorcentagem = this.converterToFloat(element['ROE']);
            novaAcao.indicadoresFundamentalistas = indicadoresFundamentalistas;

            const dadosBalancoPatrimonial = new DadosBalancoPatrimonialBancario();
            dadosBalancoPatrimonial.ativo = this.converterToFloat(element['Ativo']);
            dadosBalancoPatrimonial.patrimonioLiquido = this.converterToFloat(element['Patrim. Líq']);
            novaAcao.dadosBalancoPatrimonial = dadosBalancoPatrimonial;

            const dadosDemonstrativosDeResultadosUltimosTresMeses = new DadosDemonstrativosDeResultadosBancario();
            dadosDemonstrativosDeResultadosUltimosTresMeses.resultadoDeIntermediacaoFinanceira = this.converterToInt(element['Result Int Financ Ultimos Tres Meses']);
            dadosDemonstrativosDeResultadosUltimosTresMeses.receitaDeServicos = this.converterToInt(element['Rec Serviços Ultimos Tres Meses']);
            dadosDemonstrativosDeResultadosUltimosTresMeses.lucroLiquido = this.converterToInt(element['Lucro Líquido Ultimos Tres Meses']);
            novaAcao.dadosDemonstrativosDeResultadosUltimosTresMeses = dadosDemonstrativosDeResultadosUltimosTresMeses;

            const dadosDemonstrativosDeResultadosUltimosDozeMeses = new DadosDemonstrativosDeResultadosBancario();
            dadosDemonstrativosDeResultadosUltimosDozeMeses.resultadoDeIntermediacaoFinanceira = this.converterToInt(element['Result Int Financ Ultimos Dozes Meses']);
            dadosDemonstrativosDeResultadosUltimosDozeMeses.receitaDeServicos = this.converterToInt(element['Rec Serviços Ultimos Dozes Meses']);
            dadosDemonstrativosDeResultadosUltimosDozeMeses.lucroLiquido = this.converterToInt(element['Lucro Líquido Ultimos Dozes Meses']);
            novaAcao.dadosDemonstrativosDeResultadosUltimosDozeMeses = dadosDemonstrativosDeResultadosUltimosDozeMeses;

            acoesCompletas.push(novaAcao);
        });

        return acoesCompletas;
    }
}