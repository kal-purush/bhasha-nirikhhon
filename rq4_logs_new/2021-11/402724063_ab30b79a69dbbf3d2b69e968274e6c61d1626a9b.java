package seedu.academydirectory.logic.commands;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static seedu.academydirectory.testutil.TypicalStudents.getTypicalAcademyDirectory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import seedu.academydirectory.model.AcademyDirectory;
import seedu.academydirectory.model.UserPrefs;
import seedu.academydirectory.model.VersionedModel;
import seedu.academydirectory.model.VersionedModelManager;

public class HistoryCommandTest {
    private static final Path COMMAND_TEST_DIR = Paths.get("src", "test", "data",
            "VersionControlTest", "CommandTest");
    private static final AcademyDirectory TYPICAL_ACADEMY = getTypicalAcademyDirectory();
    private static final AcademyDirectory EMPTY_ACADEMY = new AcademyDirectory();
    private static final Supplier<Stream<AcademyDirectory>> academyDirectorySupplier = () -> Stream.of(
            TYPICAL_ACADEMY, EMPTY_ACADEMY);

    @Test
    public void execute_noVcFiles(@TempDir Path tempPath) {
        UserPrefs userPrefs = getTempUserPref(COMMAND_TEST_DIR.resolve("EmptyVCFolder"), tempPath);
        academyDirectorySupplier.get()
                .forEach((academyDirectory) -> {
                    VersionedModel model = new VersionedModelManager(academyDirectory, userPrefs);
                    Command historyCommand = new HistoryCommand();
                    CommandResult commandResult = assertDoesNotThrow(() -> historyCommand.execute(model));
                    assertEquals(HistoryCommand.MESSAGE_SUCCESS, commandResult.getFeedbackToUser());
                    assertEquals("", model.getAdditionalViewModel().getAdditionalInfo().get());
                });
    }

    @Test
    public void execute_missingVcFolder(@TempDir Path tempPath) {
        UserPrefs userPrefs = getTempUserPref(tempPath.resolve("IMMMISSING"), tempPath);
        academyDirectorySupplier.get()
                .forEach((academyDirectory) -> {
                    VersionedModel model = new VersionedModelManager(academyDirectory, userPrefs);
                    Command historyCommand = new HistoryCommand();
                    CommandResult commandResult = assertDoesNotThrow(() -> historyCommand.execute(model));
                    assertEquals(HistoryCommand.MESSAGE_SUCCESS, commandResult.getFeedbackToUser());
                    assertEquals("", model.getAdditionalViewModel().getAdditionalInfo().get());
                });
    }

    @Test
    public void execute_linearHistory(@TempDir Path tempPath) {
        UserPrefs userPrefs = getTempUserPref(COMMAND_TEST_DIR.resolve("LinearHistory"), tempPath);
        String expectedResult = "| * 09726 - Sun, 7 Nov 2021 12:50:21 +0800 (HEAD) (MAIN)\n"
                + "| | \t\tDeleted Student: Charlotte Oliveiro\n"
                + "|/\n"
                + "* b1d61 - Sun, 7 Nov 2021 12:50:19 +0800 (OLD)\n"
                + "| \t\tDeleted Student: Bernice Yu\n"
                + "* f16ed - Sun, 7 Nov 2021 12:50:17 +0800 \n"
                + "| \t\tDeleted Student: Alex Yeoh\n"
                + "* b91d4 - Sun, 7 Nov 2021 12:50:04 +0800 \n"
                + "| \t\tInitial Commit";

        academyDirectorySupplier.get()
                .forEach(academyDirectory -> {
                    VersionedModel model = new VersionedModelManager(academyDirectory, userPrefs);
                    Command historyCommand = new HistoryCommand();
                    CommandResult commandResult = assertDoesNotThrow(() -> historyCommand.execute(model));
                    assertEquals(HistoryCommand.MESSAGE_SUCCESS, commandResult.getFeedbackToUser());
                    assertEquals(expectedResult, model.getAdditionalViewModel().getAdditionalInfo().get());
                });
    }

    @Test
    public void execute_branchHistory(@TempDir Path tempPath) {
        UserPrefs userPrefs = getTempUserPref(COMMAND_TEST_DIR.resolve("BranchHistory"), tempPath);

        String expectedResult = "| * a4856 - Sun, 7 Nov 2021 12:54:33 +0800 (HEAD) (MAIN)\n"
                + "| | \t\tDeleted Student: Irfan Ibrahim\n"
                + "* | 50499 - Sun, 7 Nov 2021 12:54:14 +0800 (OLD)\n"
                + "| | \t\tDeleted Student: David Li\n"
                + "* | fffd2 - Sun, 7 Nov 2021 12:54:10 +0800 \n"
                + "| | \t\tDeleted Student: Roy Balakrishnan\n"
                + "|/\n"
                + "* b1d61 - Sun, 7 Nov 2021 12:50:19 +0800 \n"
                + "| \t\tDeleted Student: Bernice Yu\n"
                + "* f16ed - Sun, 7 Nov 2021 12:50:17 +0800 \n"
                + "| \t\tDeleted Student: Alex Yeoh\n"
                + "* b91d4 - Sun, 7 Nov 2021 12:50:04 +0800 \n"
                + "| \t\tInitial Commit";

        academyDirectorySupplier.get()
                .forEach(academyDirectory -> {
                    VersionedModel model = new VersionedModelManager(academyDirectory, userPrefs);
                    Command historyCommand = new HistoryCommand();
                    CommandResult commandResult = assertDoesNotThrow(() -> historyCommand.execute(model));
                    assertEquals(HistoryCommand.MESSAGE_SUCCESS, commandResult.getFeedbackToUser());
                    assertEquals(expectedResult, model.getAdditionalViewModel().getAdditionalInfo().get());
                });
    }

    private static UserPrefs getTempUserPref(Path path, Path tempPath) {
        UserPrefs userPrefs = new UserPrefs();
        userPrefs.setAcademyDirectoryFilePath(tempPath);
        userPrefs.setVersionControlPath(path);
        return userPrefs;
    }
}