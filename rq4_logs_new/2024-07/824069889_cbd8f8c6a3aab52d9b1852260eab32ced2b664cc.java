package cnsa.demo.DTO.workspaceDTO;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Getter
@NoArgsConstructor
public class WorkSpaceWithDateDTO {
    private LocalDate date;
    private List<WorkspaceDTO> workspaces;

    @Builder
    public WorkSpaceWithDateDTO(LocalDate localDate, List<WorkspaceDTO> workspaceDTOS) {
        this.date=localDate;
        this.workspaces=workspaceDTOS;
    }
}