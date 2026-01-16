package groupone.userservice.dto.request;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class GeneralInfoRequest {
    private List<Long> userIdList;
}