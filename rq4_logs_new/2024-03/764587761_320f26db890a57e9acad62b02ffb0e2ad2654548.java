package km.cd.backend.challenge.dto;

import km.cd.backend.challenge.domain.Challenge;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
public class ChallengeDTO {
    private String challenge_name;
    private String  start_date;
    private String end_date;
    private Integer certification_frequency;
    private String certification_explanation;
    private Integer certification_count;
    private String certification_method;
    private String challenge_explanation;
    private Integer maximum_people;
    private Boolean is_private;
    private String private_code;

    public Challenge toEntity() {
        return Challenge.builder()
                .challenge_name(challenge_name)
                .start_date(parseDateString(start_date))
                .end_date(parseDateString(end_date))
                .certification_frequency(certification_frequency)
                .certification_explanation(certification_explanation)
                .certification_count(certification_count)
                .certification_method(certification_method)
                .challenge_explanation(challenge_explanation)
                .maximum_people(maximum_people)
                .is_private(is_private)
                .private_code(private_code)
                .build();
    }

    private Date parseDateString(String dateString) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            return simpleDateFormat.parse(dateString);
        } catch (ParseException e) {
            return null;
        }
    }
}