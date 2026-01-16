package org.springframework.samples.gitvision.change;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHTree;
import org.kohsuke.github.GHTreeEntry;
import org.kohsuke.github.GitHub;
import org.springframework.samples.gitvision.change.model.Change;
import org.springframework.stereotype.Service;

@Service
public class ChangeService {
    
    
    public void getChangesUserByFile() throws IOException{
        GitHub gitHub = GitHub.connectAnonymously();
        GHRepository ghRepository = gitHub.getRepository(null);

        List<GHCommit> commits = ghRepository.listCommits().toList();
        Map<String, Map<String, Change>> fileChangesByUser = new HashMap<>();

        Set<String> existingFiles = new HashSet<>();
        GHTree tree = ghRepository.getTreeRecursive("HEAD", -1); 
        for (GHTreeEntry entry : tree.getTree()) {
            if ("blob".equals(entry.getType())) {
                existingFiles.add(entry.getPath());
            }
        }

        for (GHCommit commit : commits) {
            String author = commit.getCommitShortInfo().getAuthor().getName(); 
            List<GHCommit.File> files = commit.listFiles().toList();

            for (GHCommit.File file : files) {
                String fileName = file.getFileName();
                String status = file.getStatus();

                if ("renamed".equals(status)) {
                    fileName = file.getPreviousFilename();
                }

                if(!existingFiles.contains(fileName))
                    continue;

                fileChangesByUser
                    .computeIfAbsent(fileName, k -> new HashMap<>())
                    .merge(author, Change.of(file.getLinesAdded(), file.getLinesDeleted()), Change::merge);
            }
        }

    }

}