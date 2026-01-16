package seedu.academydirectory.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import seedu.academydirectory.commons.util.FileUtil;
import seedu.academydirectory.storage.StageAreaStorage;
import seedu.academydirectory.versioncontrol.objects.Commit;
import seedu.academydirectory.versioncontrol.objects.Label;
import seedu.academydirectory.versioncontrol.objects.Tree;
import seedu.academydirectory.versioncontrol.objects.VcObject;
import seedu.academydirectory.versioncontrol.utils.HashMethod;

// Integration Test of VersionControl with other components
class VersionControlTest {
    private static final Path RELEVANT_DIR = Paths.get("src", "test",
            "data", "VersionControlTest", "VersionControlTest");
    private static final Path COMMIT_DIR = RELEVANT_DIR.resolve(Paths.get("Commit"));
    private static final Path REVERT_DIR = RELEVANT_DIR.resolve(Paths.get("Revert"));

    @TempDir
    public Path tempPath;

    @TempDir
    public Path noWritePath;

    @Test
    public void constructor_initialCommitPresent_fetchCorrectHash() {
        VersionControl versionControl = new VersionControl(HashMethod.SHA1, COMMIT_DIR, tempPath);
        String commitHash = "70e794b5c26973d17a17b83d2ff2a30b0669c920";
        String commitMessage = "Edited Student: Bernice Yu";

        Commit fetchedHeadCommit = versionControl.getHeadCommit();
        assertEquals(commitHash, fetchedHeadCommit.getHash());
        assertEquals(commitMessage, fetchedHeadCommit.getMessage());
    }

    @Test
    public void constructor_initialCommitAbsent_createInitialCommit() {
        Date dateBeforeCommit = new Date();
        VersionControl versionControl = new VersionControl(HashMethod.SHA1, tempPath, tempPath);
        Date dateAfterCommit = new Date();

        Commit fetchedHeadCommit = versionControl.getHeadCommit();
        assertEquals(System.getProperty("user.name"), fetchedHeadCommit.getAuthor());
        assertEquals("Initial Commit", fetchedHeadCommit.getMessage());
        assertTrue(fetchedHeadCommit.getDate().compareTo(dateBeforeCommit) >= 0);
        assertTrue(fetchedHeadCommit.getDate().compareTo(dateAfterCommit) <= 0);
    }

    @Test
    public void commit_correctStoragePath() {
        for (File file : Objects.requireNonNull(COMMIT_DIR.toFile().listFiles())) {
            assertDoesNotThrow(() -> Files.copy(file.toPath(), tempPath.resolve(file.toPath().getFileName())));
        }
        Path dataDir = tempPath.resolve(Paths.get("Data"));
        assertDoesNotThrow(() -> FileUtil.createFile(dataDir));
        VersionControl versionControl = new VersionControl(HashMethod.SHA1, tempPath, dataDir);

        Commit prevHeadCommit = versionControl.getHeadCommit();

        String commitMessage = "TESTING";
        versionControl.commit(commitMessage);

        // internal head tracker should be shifted
        assertNotEquals(prevHeadCommit, versionControl.getHeadCommit());

        StageAreaStorage stageAreaStorage = new StageAreaStorage(tempPath);
        assertDoesNotThrow(() -> stageAreaStorage.saveStageArea(versionControl.getStageArea()));
        assertTrue(FileUtil.isFileExists(tempPath.resolve(VersionControl.OLD_LABEL_STRING)));
        assertTrue(FileUtil.isFileExists(tempPath.resolve(VersionControl.CURRENT_LABEL_STRING)));
        assertTrue(FileUtil.isFileExists(tempPath.resolve(VersionControl.HEAD_LABEL_STRING)));

        // Labels should be understandable
        Commit fetchedHeadCommit = versionControl.fetchCommitByLabel(VersionControl.HEAD_LABEL_STRING);
        assertEquals(versionControl.getHeadCommit(), fetchedHeadCommit);
        assertEquals(prevHeadCommit, fetchedHeadCommit.getParentSupplier().get());
        assertEquals(System.getProperty("user.name"), fetchedHeadCommit.getAuthor());
        assertEquals(commitMessage, fetchedHeadCommit.getMessage());
        assertEquals(prevHeadCommit, fetchedHeadCommit.getParentSupplier().get());
        assertTrue(FileUtil.isFileExists(tempPath.resolve(fetchedHeadCommit.getHash())));

        Commit fetchedCurrentCommit = versionControl.fetchCommitByLabel(VersionControl.CURRENT_LABEL_STRING);
        assertEquals(fetchedHeadCommit, fetchedCurrentCommit);

        Commit fetchedOldCommit = versionControl.fetchCommitByLabel(VersionControl.OLD_LABEL_STRING);
        assertEquals(prevHeadCommit, fetchedOldCommit);

        // Check committed tree
        Tree blobTree = fetchedHeadCommit.getTreeSupplier().get();
        assertEquals(dataDir.toString(),
                blobTree.getHashMap().get(blobTree.getHashMap().keySet().stream().findFirst().orElse(null)));
        assertTrue(FileUtil.isFileExists(Path.of(blobTree.getHashMap().keySet().stream().findFirst().orElse(""))));
    }

    @Test
    public void commit_incorrectStoragePath() {
        for (File file : Objects.requireNonNull(COMMIT_DIR.toFile().listFiles())) {
            assertDoesNotThrow(() -> Files.copy(file.toPath(), tempPath.resolve(file.toPath().getFileName())));
        }
        VersionControl versionControl = new VersionControl(HashMethod.SHA1, tempPath, tempPath);

        Commit prevHeadCommit = versionControl.getHeadCommit();

        String commitMessage = "TESTING";
        versionControl.commit(commitMessage);

        // internal head tracker should be shifted
        assertNotEquals(prevHeadCommit, versionControl.getHeadCommit());

        // Tree.NULL -> unable to write tree
        StageAreaStorage stageAreaStorage = new StageAreaStorage(tempPath);
        assertThrows(IOException.class, () -> stageAreaStorage.saveStageArea(versionControl.getStageArea()));

        // Everything else should still be correct
        assertTrue(FileUtil.isFileExists(tempPath.resolve(VersionControl.OLD_LABEL_STRING)));
        assertTrue(FileUtil.isFileExists(tempPath.resolve(VersionControl.CURRENT_LABEL_STRING)));
        assertTrue(FileUtil.isFileExists(tempPath.resolve(VersionControl.HEAD_LABEL_STRING)));

        Commit fetchedHeadCommit = versionControl.fetchCommitByLabel(VersionControl.HEAD_LABEL_STRING);
        assertEquals(versionControl.getHeadCommit(), fetchedHeadCommit);
        assertEquals(prevHeadCommit, fetchedHeadCommit.getParentSupplier().get());
        assertEquals(System.getProperty("user.name"), fetchedHeadCommit.getAuthor());
        assertEquals(commitMessage, fetchedHeadCommit.getMessage());
        assertEquals(prevHeadCommit, fetchedHeadCommit.getParentSupplier().get());
        assertTrue(FileUtil.isFileExists(tempPath.resolve(fetchedHeadCommit.getHash())));

        Commit fetchedCurrentCommit = versionControl.fetchCommitByLabel(VersionControl.CURRENT_LABEL_STRING);
        assertEquals(fetchedHeadCommit, fetchedCurrentCommit);

        Commit fetchedOldCommit = versionControl.fetchCommitByLabel(VersionControl.OLD_LABEL_STRING);
        assertEquals(prevHeadCommit, fetchedOldCommit);

        // Tree is Null
        Tree blobTree = fetchedHeadCommit.getTreeSupplier().get();
        assertEquals(Tree.NULL, blobTree);
    }

    @Test
    public void revert_negativeTests() {
        for (File file : Objects.requireNonNull(REVERT_DIR.toFile().listFiles())) {
            assertDoesNotThrow(() -> Files.copy(file.toPath(), tempPath.resolve(file.toPath().getFileName())));
        }
        Path dataDir = tempPath.resolve(Paths.get("Data"));
        assertDoesNotThrow(() -> FileUtil.createFile(dataDir));
        VersionControl versionControl = new VersionControl(HashMethod.SHA1, tempPath, dataDir);

        // Reverting to current hash -> Returns null commit
        assertEquals(Commit.NULL,
                assertDoesNotThrow(() -> versionControl.revert(versionControl.getHeadCommit().getHash())));

        // Reverting to a commit that is not present -> Returns null commit
        assertEquals(Commit.NULL,
                assertDoesNotThrow(() -> versionControl.revert("MISSING")));
    }

    @Test
    public void revert_correctHash() {
        for (File file : Objects.requireNonNull(REVERT_DIR.toFile().listFiles())) {
            assertDoesNotThrow(() -> Files.copy(file.toPath(), tempPath.resolve(file.toPath().getFileName())));
        }
        Path regeneratePath = RELEVANT_DIR.resolve("temp").resolve("academydirectory.json");
        if (FileUtil.isFileExists(regeneratePath)) {
            assertTrue(regeneratePath.toFile().delete());
        }

        Path dataDir = tempPath.resolve(Paths.get("Data"));
        assertDoesNotThrow(() -> FileUtil.createFile(dataDir));
        VersionControl versionControl = new VersionControl(HashMethod.SHA1, tempPath, dataDir);
        Commit currHead = versionControl.getHeadCommit();

        // Reverting to a commit that is present -> Returns that commit
        String correctHash = "6b8dca90ac26ec6f2f4fc3b7f820bc57f462fcf9";
        Commit revertedCommit = assertDoesNotThrow(() -> versionControl.revert(correctHash));

        assertNotEquals(Commit.NULL, revertedCommit);
        assertNotEquals(currHead, revertedCommit);
        assertEquals(revertedCommit, versionControl.getHeadCommit());

        // Blob regenerated correctly
        Path path = assertDoesNotThrow(() -> Paths.get(Objects.requireNonNull(revertedCommit.getTreeSupplier().get()
                .getHashMap().keySet().stream().findFirst().orElse(null))));
        assertTrue(FileUtil.isFileExists(path));
        byte[] regeneratedData = assertDoesNotThrow(() -> Files.readAllBytes(path));
        byte[] expectedData = assertDoesNotThrow(() -> Files.readAllBytes(REVERT_DIR.resolve("academydirectory.json")));
        assertArrayEquals(expectedData, regeneratedData);

        // HEAD label is staged
        List<VcObject> vcObjectList = versionControl.getStageArea().getVcObjectList();
        assertEquals(1, vcObjectList.size());
        assertTrue(vcObjectList.get(0) instanceof Label);
        assertEquals(revertedCommit, ((Label) vcObjectList.get(0)).getCommitSupplier().get());
    }
}