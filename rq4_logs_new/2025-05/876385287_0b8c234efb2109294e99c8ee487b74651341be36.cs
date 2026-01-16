using LangSharp.Core.Interfaces.Services;
using LangSharp.IntegrationTests.Fixtures;
using Xunit.Abstractions;

namespace LangSharp.IntegrationTests.Services
{
    public class LangSharpService_CallAIChat_LangChainGemini20Flash_Tests : IClassFixture<LangChainGemini20FlashLangSharpServiceFixture>
    {
        #region [Prompt]
        private const string Prompt = @"
            Conteúdo é: Padrões Sistemas RIS e PACS
            linguagem formal
            características de texto científico (rico em informações)
            abordagem dialógica
            permitido: primeira pessoa do plural (nós): serve para o autor se referir a si mesmo;
            permitido: segunda pessoa do singular (você): faz referência ao aluno.
            frases que tenham entre 5 a 15 palavras.
            cada parágrafo deve ter mais de 100 palavras
            Evite esse tipo de expressão: Nós compreendemo
            Deve ter no mínimo 8 parágrafos
            o texto deve ser longo
            não use listas
            Explique os exemplos, detalhe por detalhe, linha por linha
            o texto deve ser longo
            explique os exemplos que você deu
            Divida o texto em quatro subtítulos, você decide como
            cite a seguinte bibliografia: COLICCHIO, Tiago Kuse. Introdução à informática em saúde: fundamentos, aplicações e lições aprendidas com a informatização do sistema de saúde americano. Porto Alegre: Artmed, 2020. (BVMB)
            NÃO DEVE fazer citação direta. Citações assim: o por Colicchio (2020), a interoperabilidade.
            Deve ter apenas paráfrase. Segue um exemplo (COLICCHIO, 2020)
            Deve ter exemplo de código
            Explique todos os elementos do código em forma de parágrafo, adicione comentários no código
            Segue um exemplo de como a citação DEVE ser:  código, pois grande parte da (COLICCHIO, 2020)
            Segue um exemplo de como a citação NÃO deve ser:  aptável. Segundo Escudelario e Pinho (2021), a utilização do flexbox
            Não deve ter essa frase: Essa técnica é amplamente abordada em Desenvolvimento de Software II
            Você não deve fazer afirmações históricas como (ialmente pela Microsoft em 1999.), mantenha o foco apenas na descrição da funcionalidade e como ela opera tecnicamente
            Não faça um parágrafo assim (Axios tornou-se a biblioteca preferida para requisições HTTP em aplicações modernas:) fornecendo logo o exemplo, antes do exemplo você deve explicar a tecnologia com detalhes, com parágrafos grandes ( mais de 100 palavras)";
        #endregion

        private readonly ILangSharpService _service;
        private readonly ITestOutputHelper _output;
        public LangSharpService_CallAIChat_LangChainGemini20Flash_Tests(
          LangChainGemini20FlashLangSharpServiceFixture fixture,
          ITestOutputHelper output)
        {
            _service = fixture.Service;
            _output = output;
        }

        [Fact(DisplayName = "Null prompt returns error from RequestValidatorHandler")]
        public void CallAIChatAsync_NullPrompt_ReturnsError()
        {
            var result = _service.CallAIChat(null!);
            var resultString = result as string;

            Assert.NotNull(resultString);
            Assert.Contains("Error", resultString, StringComparison.OrdinalIgnoreCase);
        }

        [Fact(DisplayName = "Empty prompt returns error from RequestValidatorHandler")]
        public void CallAIChatAsync_EmptyPrompt_ReturnsError()
        {
            var result = _service.CallAIChat(string.Empty);
            var resultString = result as string;

            _output.WriteLine("AI Response:\n" + resultString);

            Assert.NotNull(resultString);
            Assert.Contains("Error: Request parameter is empty.", resultString, StringComparison.OrdinalIgnoreCase);
        }

        [Fact(DisplayName = "Professor prompt returns lesson plan")]
        public void CallAIChatAsync_ProfessorPrompt_ReturnsLessonPlan()
        {
            var result = _service.CallAIChat(Prompt);
            var resultString = result as string;

            _output.WriteLine(resultString);

            Assert.NotNull(resultString);
            Assert.Contains("RIS", resultString, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("PACS", resultString, StringComparison.OrdinalIgnoreCase);
        }
    }
}