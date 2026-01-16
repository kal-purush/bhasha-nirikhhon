package com.example.issuetrackingsystem.service;

import com.example.issuetrackingsystem.domain.*;
import com.example.issuetrackingsystem.domain.enums.ProjectAccountRole;
import com.example.issuetrackingsystem.domain.enums.ProjectStatus;
import com.example.issuetrackingsystem.domain.key.ProjectAccountPK;
import com.example.issuetrackingsystem.dto.*;
import com.example.issuetrackingsystem.exception.ErrorCode;
import com.example.issuetrackingsystem.exception.ITSException;
import com.example.issuetrackingsystem.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ProjectServiceImplTest {

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private ProjectRepository projectRepository;

    @Mock
    private IssueRepository issueRepository;

    @Mock
    private ProjectAccountRepository projectAccountRepository;

    @Mock
    private CommentRepository commentRepository;

    @InjectMocks
    private ProjectServiceImpl projectService;

    @Test
    void testGetProjectListAsAdmin() {
        List<Project> projects = Arrays.asList(
            Project.builder().projectId(1L).title("Project 1").status(ProjectStatus.IN_PROGRESS).build(),
            Project.builder().projectId(2L).title("Project 2").status(ProjectStatus.NOT_STARTED).build()
        );

        when(projectRepository.findAll()).thenReturn(projects);

        ProjectResponse response = projectService.getProjectList(1L);
        assertEquals(1, response.getIsAdmin());
        assertEquals(2, response.getProject().size());
    }

    @Test
    void testGetProjectListAsUser() {
        List<Project> projects = Arrays.asList(
            Project.builder().projectId(1L).title("Project 1").status(ProjectStatus.IN_PROGRESS).build()
        );

        when(projectRepository.findByAccountId(2L)).thenReturn(projects);

        ProjectResponse response = projectService.getProjectList(2L);
        assertEquals(0, response.getIsAdmin());
        assertEquals(1, response.getProject().size());
    }

    @Test
    void testFindProjectAsAdmin() {
        Project project = Project.builder().projectId(1L).title("Project 1").description("Description").status(ProjectStatus.IN_PROGRESS).date(LocalDateTime.now()).build();
        when(projectRepository.findById(1L)).thenReturn(Optional.of(project));
        when(issueRepository.findByProjectId(1L)).thenReturn(Collections.emptyList());
        when(projectAccountRepository.findById_ProjectId(1L)).thenReturn(Collections.emptyList());

        DetailsProjectResponse response = projectService.findProject(1L, 1L);
        assertNotNull(response);
        assertEquals(-1, response.getAccountRole());
    }

    @Test
    void testFindProjectAsUser() {
        ProjectAccount projectAccount = ProjectAccount.builder()
            .id(ProjectAccountPK.builder().accountId(2L).projectId(1L).build())
            .role(ProjectAccountRole.dev)
            .build();

        Project project = Project.builder().projectId(1L).title("Project 1").description("Description").status(ProjectStatus.IN_PROGRESS).date(LocalDateTime.now()).build();
        when(projectAccountRepository.findById(any())).thenReturn(Optional.of(projectAccount));
        when(projectRepository.findById(1L)).thenReturn(Optional.of(project));
        when(issueRepository.findByProjectId(1L)).thenReturn(Collections.emptyList());
        when(projectAccountRepository.findById_ProjectId(1L)).thenReturn(Collections.emptyList());

        DetailsProjectResponse response = projectService.findProject(1L, 2L);
        assertNotNull(response);
        assertEquals(ProjectAccountRole.dev.ordinal(), response.getAccountRole());
    }

    @Test
    void testAddProjectAsAdmin() {
        Account account = Account.builder().accountId(1L).username("admin").build();
        AddProjectRequest.ProjectMemberData memberData = AddProjectRequest.ProjectMemberData.builder()
            .username("user")
            .role(1)
            .build();
        AddProjectRequest addProjectRequest = AddProjectRequest.builder()
            .title("New Project")
            .description("Description")
            .member(Collections.singletonList(memberData))
            .build();

        Project project = Project.builder().projectId(1L).title("New Project").description("Description").build();
        when(accountRepository.findByUsername("user")).thenReturn(Optional.of(account));
        when(projectRepository.save(any(Project.class))).thenReturn(project);

        String result = projectService.addProject(1L, addProjectRequest);
        assertEquals("/projects/1", result);
    }

    @Test
    void testAddProjectForbidden() {
        AddProjectRequest addProjectRequest = AddProjectRequest.builder()
            .title("New Project")
            .description("Description")
            .build();

        ITSException exception = assertThrows(ITSException.class, () -> projectService.addProject(2L, addProjectRequest));

        assertEquals(ErrorCode.PROJECT_CREATION_FORBIDDEN, exception.getErrorCode());
    }

    @Test
    void testModifyProjectAsAdmin() {
        ModifyProjectRequest modifyProjectRequest = ModifyProjectRequest.builder()
            .title("Updated Project")
            .build();

        Project project = Project.builder()
            .projectId(1L)
            .title("Old Project")
            .description("Old Description")
            .status(ProjectStatus.IN_PROGRESS)
            .date(LocalDateTime.now())
            .build();

        when(projectRepository.findById(1L)).thenReturn(Optional.of(project));
        when(projectRepository.save(any(Project.class))).thenAnswer(invocation -> {
            Project savedProject = invocation.getArgument(0);
            try {
                Field titleField = Project.class.getDeclaredField("title");
                titleField.setAccessible(true);
                titleField.set(project, savedProject.getTitle());
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            return project;
        });

        projectService.modifyProject(1L, 1L, modifyProjectRequest);

        assertEquals("Updated Project", project.getTitle());
    }

    @Test
    void testModifyProjectForbidden() {
        ModifyProjectRequest modifyProjectRequest = ModifyProjectRequest.builder()
            .title("Updated Project")
            .build();

        ITSException exception = assertThrows(ITSException.class, () -> projectService.modifyProject(2L, 1L, modifyProjectRequest));

        assertEquals(ErrorCode.PROJECT_UPDATE_FORBIDDEN, exception.getErrorCode());
    }

    @Test
    void testFindProjectTrendNewIssue() {
        Long accountId = 1L;
        Long projectId = 1L;
        String category = "new-issue";

        when(issueRepository.countByReportedDate_Day(any(), any())).thenReturn(Collections.emptyList());
        when(issueRepository.countByReportedDate_Month(any(), any())).thenReturn(Collections.emptyList());

        ProjectTrendResponse response = projectService.findProjectTrend(accountId, projectId, category);

        assertNotNull(response);
        assertNotNull(response.getDaily());
        assertNotNull(response.getMonthly());
    }

    @Test
    void testFindProjectTrendForbidden() {
        Long accountId = 2L;
        Long projectId = 1L;
        String category = "new-issue";

        when(projectAccountRepository.findById(any())).thenReturn(Optional.empty());

        ITSException exception = assertThrows(ITSException.class, () -> projectService.findProjectTrend(accountId, projectId, category));

        assertEquals(ErrorCode.PROJECT_TREND_FORBIDDEN, exception.getErrorCode());
    }

    @Test
    void testFindProjectDetailNotFound() {
        when(projectRepository.findById(any())).thenReturn(Optional.empty());

        ITSException exception = assertThrows(ITSException.class, () -> projectService.findProject(1L, 1L));

        assertEquals(ErrorCode.PROJECT_DETAIL_NOT_FOUND, exception.getErrorCode());
    }

    @Test
    void testFindProjectDetailForbidden() {
        when(projectAccountRepository.findById(any())).thenReturn(Optional.empty());

        ITSException exception = assertThrows(ITSException.class, () -> projectService.findProject(1L, 2L));

        assertEquals(ErrorCode.PROJECT_DETAIL_FORBIDDEN, exception.getErrorCode());
    }
}