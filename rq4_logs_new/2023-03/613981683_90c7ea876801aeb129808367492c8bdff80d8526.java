package com.redhat.devtools.intellij.jkube.actions;

import com.intellij.ide.browsers.actions.WebPreviewVirtualFile;
import com.intellij.notification.Notification;
import com.intellij.notification.NotificationAction;
import com.intellij.notification.NotificationType;
import com.intellij.notification.Notifications;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.fileEditor.FileEditorManager;
import com.intellij.openapi.project.DumbAware;
import com.intellij.openapi.project.Project;
import com.intellij.openapi.ui.Messages;
import com.intellij.testFramework.LightVirtualFile;
import com.intellij.util.Urls;
import com.redhat.devtools.intellij.common.actions.StructureTreeAction;
import com.redhat.devtools.intellij.jkube.Constants;
import com.redhat.devtools.intellij.jkube.window.PortNode;
import com.redhat.devtools.intellij.jkube.window.TerminalLogger;
import org.eclipse.jkube.kit.remotedev.RemoteDevelopmentConfig;
import org.eclipse.jkube.kit.remotedev.RemoteDevelopmentService;
import org.eclipse.jkube.kit.remotedev.RemoteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.tree.TreePath;

public class OpenBrowserAction extends StructureTreeAction implements DumbAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenBrowserAction.class);

    public OpenBrowserAction() {
        super(PortNode.class);
    }

    @Override
    public boolean isVisible(Object selected) {
        var visible = super.isVisible(selected);
        if (visible) {
            visible = ((PortNode) selected).getHandler() != null;
        }
        return visible;
    }

    @Override
    public void actionPerformed(AnActionEvent anActionEvent, TreePath path, Object selected) {
            PortNode node = getElement(selected);
            if (node.getHandler() != null) {
                openBrowser(node.getHandler().getLocalPort(), anActionEvent.getProject());
            }
    }

    public static void openBrowser(int port, Project project) {
        var url = "http://localhost:" + port;
        var file = new LightVirtualFile(url) {
            @Override
            public int hashCode() {
                return url.hashCode();
            }

            @Override
            public boolean equals(Object obj) {
                return getName().equals(((LightVirtualFile) obj).getName());
            }
        };
        var previewFile = new WebPreviewVirtualFile(file, Urls.newFromEncoded(url));
        FileEditorManager.getInstance(project).openFile(previewFile, true, true);
    }
}