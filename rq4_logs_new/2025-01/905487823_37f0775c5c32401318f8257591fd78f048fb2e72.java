package br.ufrn.instancies.GRAB.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import br.ufrn.FormsFramework.model.Formulario;
import br.ufrn.FormsFramework.service.interfaces.FeedbackLLM;
import br.ufrn.instancies.GRAB.mapper.llm.OpenAILLMResponse;

@Component
public class FeedbackSugestaoFormularioLLM extends FeedbackLLM{
    public Map<String, String> gerarRespostaLLM(Formulario formulario){
        String prompt = gerarPrompt(formulario);

         Map<String, String> resposta = new HashMap<>();
         OpenAILLMResponse response = (OpenAILLMResponse) llmService.getRespostaFromPrompt(prompt);
         resposta.put("content", response.choices().get(0).message().content());

         return resposta;
    }
    public String gerarPrompt(Formulario formulario){
        String prompt = 
        "Considere o título \""+ formulario.getNome() +
        "\"e a descrição: \""+ formulario.getDescricao()+
        "\" de um formulário que é etapa de um processo seletivo para uma vaga de trabalho. " +
        "Dadas essas informações, crie um formulário com perguntas que visam avaliar as qualificações dos candidatos para a vaga. " +
        "Um formulário é organizado em seções, e as seções tem quesitos. " +
        "Elabore quesitos discursivos ou objetivos voltados aos temas ditos na descrição.";

        return prompt;
    }

}