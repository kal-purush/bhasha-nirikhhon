package com.redhat.devtools.intellij.jkube.actions;

import com.intellij.notification.Notification;
import com.intellij.notification.NotificationType;
import com.intellij.notification.Notifications;
import com.intellij.openapi.actionSystem.AnActionEvent;
import com.intellij.openapi.project.DumbAware;
import com.intellij.openapi.ui.Messages;
import com.redhat.devtools.intellij.common.actions.StructureTreeAction;
import com.redhat.devtools.intellij.jkube.Constants;
import com.redhat.devtools.intellij.jkube.dialogs.LocalServiceDialog;
import com.redhat.devtools.intellij.jkube.window.LocalServicesNode;
import com.redhat.devtools.intellij.jkube.window.MessageNode;
import com.redhat.devtools.intellij.jkube.window.RootNode;
import com.redhat.devtools.intellij.jkube.window.TerminalLogger;
import org.eclipse.jkube.kit.remotedev.LocalService;
import org.eclipse.jkube.kit.remotedev.RemoteDevelopmentConfig;
import org.eclipse.jkube.kit.remotedev.RemoteDevelopmentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.tree.TreePath;

public class StartLocalServiceAction extends StructureTreeAction implements DumbAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartLocalServiceAction.class);

    public StartLocalServiceAction() {
        super(LocalServicesNode.class);
    }

    @Override
    public void actionPerformed(AnActionEvent anActionEvent, TreePath path, Object selected) {
        LocalServicesNode node = getElement(selected);
        var dialog = new LocalServiceDialog(anActionEvent.getProject());
        if (dialog.showAndGet()) {
            var serviceName = dialog.getServiceName();
            var port = dialog.getPort();
            var localService = LocalService.builder()
                    .serviceName(serviceName)
                    .port(port).build();
            var config = RemoteDevelopmentConfig.builder().localService(localService).build();
            var logger = new TerminalLogger("localhost:" + port,
                    anActionEvent.getProject());
            var childNode = new MessageNode<>("Local port " + port + " exposed as service " + serviceName, node);
            node.addChild(childNode);
            var service = new RemoteDevelopmentService(logger, node.getParent().getClient(), config);
            service.start().handle((res, err) -> {
                node.removeChild(childNode);
                if (err != null) {
                    Messages.showErrorDialog(anActionEvent.getProject(), "Can't expose local port " + port +
                            "' as service " + serviceName + " error:" + err.getLocalizedMessage(), "Error");
                }
                return res;
            });
            var content = "Service " + serviceName + " available on cluster for local port " +
                    port;
            var notification = new Notification(Constants.NOTIFICATION_GROUP_ID, "JKube", content,
                    NotificationType.INFORMATION);
            Notifications.Bus.notify(notification, anActionEvent.getProject());
        }
    }
}