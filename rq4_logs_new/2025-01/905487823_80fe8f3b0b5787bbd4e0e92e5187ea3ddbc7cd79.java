package br.ufrn.instancies.DASH.service;

import java.util.List;

import org.springframework.stereotype.Service;

import br.ufrn.FormsFramework.mapper.interfaces.ILLMResponse;
import br.ufrn.FormsFramework.service.LLMService;
import br.ufrn.instancies.DASH.mapper.llm.GroqLLMMessage;
import br.ufrn.instancies.DASH.mapper.llm.GroqLLMRequest;
import br.ufrn.instancies.DASH.mapper.llm.GroqLLMResponse;

@Service
public class GroqLlamaService extends LLMService {

    @Override
    public ILLMResponse getRespostaFromPrompt(String prompt) {
        GroqLLMMessage message = new GroqLLMMessage("user", prompt);
        GroqLLMRequest request = new GroqLLMRequest(List.of(message), model, 1);

        GroqLLMResponse response = restTemplate.postForObject(apiUrl, request, GroqLLMResponse.class);

        return response;
    }
    
}