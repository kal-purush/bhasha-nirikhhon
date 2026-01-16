package cnsa.demo.DTO;

import cnsa.demo.DTO.Security.SessionUser;
import cnsa.demo.domain.LLMModel;
import cnsa.demo.domain.Workspace;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class LLMModelDTO {
    private Long modelId;
    private String modelName;
    private String modelDesc;
    private Integer maxToken;
    private Boolean streamable;
    private Double temperature;
    private String chatUrl;
    private String responseNodeAt;
    private String resultTokenAt;
    private String imageUrl;

    @Builder
    public LLMModelDTO(Long modelId, String modelName, String modelDesc, Integer maxToken, Boolean streamable,
                       Double temperature, String chatUrl, String responseNodeAt, String resultTokenAt,
                       String imageUrl) {
        this.modelId = modelId;
        this.modelName = modelName;
        this.modelDesc = modelDesc;
        this.maxToken = maxToken;
        this.streamable = streamable;
        this.temperature = temperature;
        this.chatUrl = chatUrl;
        this.responseNodeAt = responseNodeAt;
        this.resultTokenAt = resultTokenAt;
        this.imageUrl = imageUrl;
    }

    public LLMModelDTO(LLMModel llmModel) {
        this.modelId = llmModel.getModelId();
        this.modelName = llmModel.getModelName();
        this.modelDesc = llmModel.getModelDesc();
        this.maxToken = llmModel.getMaxToken();
        this.streamable = llmModel.getStreamable();
        this.temperature = llmModel.getTemperature();
        this.chatUrl = llmModel.getChatUrl();
        this.responseNodeAt = llmModel.getResponseNodeAt();
        this.resultTokenAt = llmModel.getResultTokenAt();
        this.imageUrl = llmModel.getImageUrl();
    }
}