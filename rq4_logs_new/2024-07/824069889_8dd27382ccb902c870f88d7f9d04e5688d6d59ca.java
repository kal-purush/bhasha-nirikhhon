package cnsa.demo.DTO.messageDTO;

import cnsa.demo.entity.Message;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class GlobalMessageDTO {
    private String content;
    private String role;

    @Builder
    public GlobalMessageDTO(String content, String role) {
        this.content = content;
        this.role = role;
    }

    public GlobalMessageDTO(Message message) {
        this(message.getContent(), message.getRole());
    }

    public Message convertToMessage() {
        Message message = new Message();
        message.setContent(content);
        message.setRole(role);
        return message;
    }
}