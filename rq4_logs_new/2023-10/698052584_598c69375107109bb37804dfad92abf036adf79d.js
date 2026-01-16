
document.addEventListener("DOMContentLoaded", function () {

    const inicio = document.getElementById("inicio");
    inicio.addEventListener("click", function () {
        window.location.href = "../../index.html"
    });

    const insights = [
        {
            title: "Crédito De Carbono",
            text: `Um crédito de carbono vale aproximadamente US$ 5 ou R$ 26 no Brasil. O preço
                    mundial do crédito
                    de carbono, no entanto, pode variar de US$ 1,20 a US$ 40 por tonelada de carbono, dependendo do
                    mercado que está sendo negociado.`,

            text1: `Um crédito de carbono é gerado a cada tonelada de carbono que
                    deixa de ser emitida ou é capturada da atmosfera. A partir de regras e metodologias previamente
                    selecionadas e validadas, é analisado se os carros estão realmente reduzindo as emissões.`,

            text2: `Um cliente
            procura uma empresa responsável pela venda de créditos de carbono. A partir da contratação da
            empresa, é formado um inventário de gases do efeito estufa produzidos pelo cliente`,
            link: '../../assets/high-angle-small-tree-stack-coins.jpg'
        },
        {
            title: "Venda e Analise de Dados",
            text: `A venda de dados trata-se da comercialização de dados sensíveis de um determinado usuário para diversos fins.
                 Essas informações podem ser comercializadas para diferentes segmentos (como call center, por exemplo),
                  em que as empresas conseguem se beneficiar desses dados.`,

            text1: `Axciom, Epsilon, Datalogix… embora você provavelmente nunca tenha ouvido falar dessas companhias, seu faturamento conjunto está beirando a cifra de US$ 2 bilhões. Os data brokers compram e vendem dados pessoais de todo tipo e em vários ramos de atuação`,
            link: '../../assets/pexels-anna-nekrashevich-6802049.jpg'
        },
        {
            title: "Venda De Seguro Especializado",
            text: `Com o avanço da tecnologia cada vez mais vemos o número de carros elétricos rodando no Brasil.
                 Um novo relatório do Rocky Mountain Institue (RMI) prevê que os veículos elétricos estão se popularizando tanto que dominarão o mercado global até o final desta década. 
                 De acordo com o documento, os modelos serão de 62% a 86% das vendas até 2030.`,


            text1: `Estimando um uso de 60 mil quilômetros, o modelo elétrico terá um TCO maior do que o convencional, pois terá depreciação mais rápida e mais gastos com seguro,
                  IPVA e manutenção. O seguro de um carro elétrico chega a ser o dobro da versão a combustão`,

            link: "../../assets/close-up-no-carregamento-do-carro-eletrico.jpg"
        },
    ];

    let currentInsightIndex = 0;

    const insightTitle = document.getElementById("insightTitle");
    const insightText = document.getElementById("insightText");
    const insightText1 = document.getElementById("insightText1");
    const insightText2 = document.getElementById("insightText2");





    const imgInsight = document.getElementById("imgInsight");
    const NextButton = document.getElementById("NextButton");

    function showCurrentInsight() {
        const currentInsight = insights[currentInsightIndex];
        insightTitle.textContent = currentInsight.title;
        insightText.textContent = currentInsight.text;
        insightText1.textContent = currentInsight.text1;
        insightText2.textContent = currentInsight.text2;
        imgInsight.src = currentInsight.link;

        if (insightText2.textContent) {
            debugger;
            insightText2.textContent = currentInsight.text2;
            insightText2.style.display = "revert";
        } else {
            insightText2.style.display = "none";
        }
    }

    const reload = document.getElementById("reload")

    reload.addEventListener("click", function () {
        window.location.reload();
    })


    NextButton.addEventListener("click", function () {
        currentInsightIndex = (currentInsightIndex + 1) % insights.length;
        showCurrentInsight();
    });

    showCurrentInsight();

});
