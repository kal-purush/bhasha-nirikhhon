package wiseSWlife.wiseSWlife.domain.faqCenter;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;



@Data
@Getter @Setter
public class Faq {


    private long num;


    private String title;


    private String questionMember;

    private String question;

    private String answer;

    public Faq(String title,  String questionMember, String question, String answer) {
        this.title = title;
        this.questionMember = questionMember;
        this.question = question;
        this.answer = answer;
    }
}