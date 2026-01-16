package com.psddev.ij.action;

import com.psddev.ij.util.AnnotationAccumulator;
import com.psddev.ij.util.EditorUtil;
import com.psddev.ij.util.PsdProjectUtil;
import com.psddev.ij.util.StringUtil;
import com.psddev.ij.util.ViewUtil;

import com.intellij.codeInsight.AnnotationUtil;
import com.intellij.openapi.actionSystem.AnAction;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.actionSystem.LangDataKeys;
import com.intellij.openapi.actionSystem.PlatformDataKeys;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.ui.Messages;
import com.intellij.psi.PsiFile;
import com.intellij.psi.PsiJavaFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class GoToRendererLayout extends AnAction {
    public void actionPerformed(AnActionEvent ev) {
        Project project = ev.getData(PlatformDataKeys.PROJECT);
        PsiFile file = ev.getData(LangDataKeys.PSI_FILE);
        if (file == null) {
            Messages.showInfoMessage(project, "Please select a Java class to navigate from.", "Information");
            return;
        }
        if (!(file instanceof PsiJavaFile)) {
            Messages.showInfoMessage(project, "You can only go to a Renderer LayoutPath file from a Java file", "Information");
            return;
        }

        String path = findLayoutPath((PsiJavaFile) file);
        if (path == null) {
            Messages.showInfoMessage(project, "This Java class has no @Renderer.LayoutPath annotation", "Information");
        }

        openLayoutPath(project, path);
    }

    private void openLayoutPath(Project project, String path) {
        String fileSystemPath = PsdProjectUtil.fileSystemPath(project, "/src/main/webapp" + path);

        File f = new File(fileSystemPath);
        if (f.exists()) {
            if (!f.isFile() || !f.canRead()) {
                Messages.showInfoMessage(project, "Selected file:\n\n" + fileSystemPath + "\n\nisn't a file, or isn't readable", "Information");
            }

        } else {
            int yn = Messages.showOkCancelDialog((Project) null, "Selected path:\n\n" + fileSystemPath + "\n\ndoesn't exist.  Create it?", "Missing file",
                                                 "Yes", "No", Messages.getQuestionIcon());
            if (yn == Messages.YES) {
                f = createRendererLayoutFileSkeleton(project, fileSystemPath);
            }
        }
        if (f.exists()) {
            EditorUtil.openFile(project, f);
        }
    }

    private File createRendererLayoutFileSkeleton(Project project, String path) {
        try {
            String taglibPath = StringUtil.firstNonNull(ViewUtil.getTaglibPath(project, path),
                                                        "/path/to/taglibs.jsp");

            FileWriter fw = new FileWriter(path);
            fw.write(ViewUtil.buildTaglibIncludeStatementText(taglibPath));
            fw.close();

            return new File(path);

        } catch (IOException e) {
            e.printStackTrace();  // TODO: real logging
            return null;
        }
    }

    private String findLayoutPath(PsiJavaFile file) {
        return AnnotationUtil.getStringAttributeValue(
                AnnotationAccumulator.findAnnotationIncludeSuperClasses(file, "com.psddev.cms.db.Renderer.LayoutPath"),
                "value");
    }
}