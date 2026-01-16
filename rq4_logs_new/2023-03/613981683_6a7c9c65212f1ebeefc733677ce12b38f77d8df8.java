package com.redhat.devtools.intellij.jkube.dialogs;

import com.intellij.openapi.project.Project;
import com.intellij.openapi.ui.DialogWrapper;
import com.intellij.ui.DocumentAdapter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import java.io.IOException;
import java.net.ServerSocket;

public class RemotePortDialog extends DialogWrapper {
    private JPanel contentPane;
    private com.intellij.ui.components.JBLabel serviceNameLabel;
    private JLabel remotePortLabel;
    private com.intellij.ui.components.JBTextField localPortText;

    private final DocumentAdapter adapter = new DocumentAdapter() {
        @Override
        protected void textChanged(@NotNull DocumentEvent e) {
            validate();
        }
    };

    public RemotePortDialog(String serviceName, int remotePort, int localPort, Project project) {
        super(project, false, IdeModalityType.PROJECT);
        init();
        setTitle("RemoteDev");
        serviceNameLabel.setText(serviceName);
        remotePortLabel.setText(Integer.toString(remotePort));
        localPortText.setText(Integer.toString(localPort));
        localPortText.getDocument().addDocumentListener(adapter);
        validate();
    }

    @Override
    protected @Nullable JComponent createCenterPanel() {
        return contentPane;
    }

    @Override
    public void validate() {
        super.validate();
        try {
            var port = Integer.parseInt(localPortText.getText());
            setOKActionEnabled(true);
            setErrorText(null, localPortText);
            try (var ignored = new ServerSocket(port)) {
            } catch (IOException e) {
                setOKActionEnabled(false);
                setErrorText("Port number is not available", localPortText);
            }
        } catch (NumberFormatException e) {
            setOKActionEnabled(false);
            setErrorText("Port number must be numeric", localPortText);
        }
    }

    public int getLocalPort() {
        return Integer.parseInt(localPortText.getText());
    }
}