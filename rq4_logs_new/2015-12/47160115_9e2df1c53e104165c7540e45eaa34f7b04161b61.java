package com.intellij.lang.yang.interfaces;

import com.intellij.compiler.CompilerMessageImpl;
import com.intellij.compiler.ProblemsView;
import com.intellij.compiler.impl.OneProjectItemCompileScope;
import com.intellij.openapi.compiler.CompilerMessage;
import com.intellij.openapi.compiler.CompilerMessageCategory;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.util.Key;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.util.containers.ContainerUtil;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public final class ProblemsViewUtil
{
    private static final Key<Object> PROBLEMS_VIEW_SESSION_ID_KEY = Key.create("ProblemsViewSessionKey");
    private static final Key<Object> PROBLEMS_VIEW_FILES_KEY = Key.create("ProblemsViewFiles");

    public ProblemsViewUtil()
    {
        // Utility class
    }

    @NotNull
    private static UUID getProblemsViewSessionId(@NotNull Project project)
    {
        UUID problemsViewSessionId = (UUID) project.getUserData(PROBLEMS_VIEW_SESSION_ID_KEY);
        if (problemsViewSessionId == null)
        {
            problemsViewSessionId = UUID.randomUUID();
            project.putUserData(PROBLEMS_VIEW_SESSION_ID_KEY, problemsViewSessionId);
        }
        return problemsViewSessionId;
    }

    @NotNull
    private static Set<String> getProblemsViewFiles(@NotNull Project project)
    {
        //noinspection unchecked
        Set<String> problemsViewFiles = (Set<String>) project.getUserData(PROBLEMS_VIEW_FILES_KEY);
        if (problemsViewFiles == null)
        {
            problemsViewFiles = new HashSet<String>();
            project.putUserData(PROBLEMS_VIEW_FILES_KEY, problemsViewFiles);
        }
        return problemsViewFiles;
    }

    public void addToProblemsView(
        @NotNull Project project,
        @NotNull CompilerMessageCategory messageCategory,
        @NotNull String message,
        @NotNull VirtualFile virtualFile,
        int lineNumber,
        int columnNumber)
    {
        CompilerMessage compilerMessage = new CompilerMessageImpl(
            project,
            messageCategory,
            message,
            virtualFile,
            lineNumber,
            columnNumber,
            /*navigatable=*/null
        );

        // You need to have stored off the existing session ID
        UUID problemsViewSessionId = getProblemsViewSessionId(project);

        ProblemsView problemsView = ProblemsView.SERVICE.getInstance(project);
        problemsView.addMessage(compilerMessage, problemsViewSessionId);
        Set<String> problemsViewFiles = getProblemsViewFiles(project);
        problemsViewFiles.add(virtualFile.getUrl());
    }

    public static void removeFromProblemsView(@NotNull Project project, @NotNull VirtualFile virtualFile)
    {
        Set<String> problemsViewFiles = getProblemsViewFiles(project);
        if (problemsViewFiles.contains(virtualFile.getUrl()))
        {
            ProblemsView problemsView = ProblemsView.SERVICE.getInstance(project);
            problemsView.clearOldMessages(new OneProjectItemCompileScope(project, virtualFile), UUID.randomUUID());

            problemsViewFiles.remove(virtualFile.getUrl());
        }
    }

    public static void clearProblemsView(@NotNull Project project)
    {
        Set<String> problemsViewFiles = getProblemsViewFiles(project);
        if (!ContainerUtil.isEmpty(problemsViewFiles))
        {
            ProblemsView problemsView = ProblemsView.SERVICE.getInstance(project);
            problemsView.clearProgress();
            problemsView.clearOldMessages(null, UUID.randomUUID());

            problemsViewFiles.clear();
        }
    }
}
 
 