package ru.fmtk.hlystov.examinationapp.services.examinator;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ShellBannerProvider extends DefaultBannerProvider {
    private String version;

    @Value("${application.version}")
    public void setVersion(String version) {
        this.version = version;
    }

    public String getBanner() {
        StringBuffer buf = new StringBuffer();
        buf.append("=======================================").append(OsUtils.LINE_SEPARATOR);
        buf.append("*            Examination              *").append(OsUtils.LINE_SEPARATOR);
        buf.append("=======================================").append(OsUtils.LINE_SEPARATOR);
        buf.append("Version:").append(version);
        return buf.toString();
    }

    public String getVersion() {
        return "0.0.1";
    }

    public String getWelcomeMessage() {
        return "Welcome to examination app";
    }

    public String getProviderName() {
        return "Shell Examination Banner";
    }
}