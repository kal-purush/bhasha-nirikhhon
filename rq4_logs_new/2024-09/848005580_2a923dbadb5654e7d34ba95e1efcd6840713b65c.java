package fa.training.dto.Interview;

import fa.training.enums.ResultInterview;
import fa.training.enums.ResultInterviewConvert;
import fa.training.enums.StatusInterview;
import jakarta.persistence.Convert;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InterviewDTO {
    private Long interviewId; // Có thể giữ lại nếu cần trong backend
    private String title;
    private String candidateName;
    private List<String> userNames;

    private LocalDate schedudate;
    private LocalTime schedudateFrom;
    private LocalTime schedudateTo;
    @Convert(converter = ResultInterviewConvert.class)
    private ResultInterview result;
    private StatusInterview status;
    private String jobName;
    private List<Long> idInterviews;
    private String meetingId;
    private String location;
    private String note;
    private String recruiter;





    public InterviewDTO(Long interviewId, String candidateName, String jobName, ResultInterview result, LocalDate schedudate, LocalTime schedudateFrom, LocalTime schedudateTo, StatusInterview status, String title, List<String> userNames, String meetingId,String note, String location, String recruiter) {

       this.interviewId = interviewId;
        this.candidateName = candidateName;
        this.jobName = jobName;
        this.result = result;
        this.schedudate = schedudate;
        this.schedudateFrom = schedudateFrom;
        this.schedudateTo = schedudateTo;
        this.status = status;
        this.title = title;
        this.userNames = userNames;
        this.meetingId = meetingId;
        this.note = note;
        this.location = location;
        this.recruiter = recruiter;
    }

    public InterviewDTO(Long interviewId, String candidateName, String jobName, ResultInterview result, LocalDate schedudate, LocalTime schedudateFrom, LocalTime schedudateTo, StatusInterview status, String title, List<String> userNames) {

        this.interviewId = interviewId;
        this.candidateName = candidateName;
        this.jobName = jobName;
        this.result = result;
        this.schedudate = schedudate;
        this.schedudateFrom = schedudateFrom;
        this.schedudateTo = schedudateTo;
        this.status = status;
        this.title = title;
        this.userNames = userNames;
    }

    public List<String> getUserNames() {
        return userNames;
    }

    public void setUserNames(List<String> userNames) {
        this.userNames = userNames != null ? userNames : new ArrayList<>();
    }

}