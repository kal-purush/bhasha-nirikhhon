package in;

import model.AuditLog;
import model.User;
import model.UserRole;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import service.AuditService;
import service.SearchService;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class SearchControllerTest {

    private SearchController searchController;
    private AuditService auditService;
    private SearchService searchService;

    @BeforeEach
    void setUp() {
        auditService = mock(AuditService.class);
        searchController = new SearchController(searchService, auditService);
    }

    @Test
    void testExportLogs() throws IOException {
        User user = new User(1, "admin", "password", UserRole.ADMIN);
        AuditLog log1 = new AuditLog(1, user, "Action 1", null);
        AuditLog log2 = new AuditLog(2, user, "Action 2", null);
        List<AuditLog> logs = Arrays.asList(log1, log2);
        when(auditService.getAllLogs()).thenReturn(logs);

        searchController.exportLogs();

        File file = new File("audit_logs.txt");
        assertThat(file.exists()).isTrue();

        // Clean up
        if (file.exists()) {
            file.delete();
        }
    }
}