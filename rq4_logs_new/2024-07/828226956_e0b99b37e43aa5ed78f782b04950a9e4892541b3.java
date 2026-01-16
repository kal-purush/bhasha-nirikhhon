package adhd.diary.chatgpt.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Usage {

    @JsonProperty("prompt_tokens")
    long promptTokens;

    @JsonProperty("completion_tokens")
    long completionTokens;

    @JsonProperty("total_tokens")
    long totalTokens;

    public Usage(long promptTokens,
                 long completionTokens,
                 long totalTokens) {
        this.promptTokens = promptTokens;
        this.completionTokens = completionTokens;
        this.totalTokens = totalTokens;
    }

    public long getPromptTokens() {
        return promptTokens;
    }

    public long getCompletionTokens() {
        return completionTokens;
    }

    public long getTotalTokens() {
        return totalTokens;
    }
}