package cnsa.demo.DTO.workspaceDTO;

import cnsa.demo.domain.User;
import cnsa.demo.domain.Workspace;
import jakarta.persistence.*;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@NoArgsConstructor
public class WorkspaceDTO {
    private Long id;
    private String workspaceName;
    private LocalDateTime createdAt;
    private LocalDateTime editedAt;
    private User user;

    @Builder
    public WorkspaceDTO(Long id, String workspaceName, LocalDateTime createdAt, LocalDateTime editedAt, User user) {
        this.id=id;
        this.workspaceName=workspaceName;
        this.createdAt=createdAt;
        this.editedAt=editedAt;
        this.user=user;
    }

    public WorkspaceDTO(Workspace workspace) {
        this(workspace.getId(), workspace.getWorkspaceName(), workspace.getCreatedAt(), workspace.getEditedAt(), workspace.getUser());
    }
}