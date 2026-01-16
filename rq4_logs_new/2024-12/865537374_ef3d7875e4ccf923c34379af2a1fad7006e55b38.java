package otus.spring.config;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Locale;
import java.util.Map;

@ConfigurationProperties(prefix = "quiz")
public class AppConfiguration implements TestConfig, TestFileNameProvider, LocaleProvider {

    private final int rightAnswersCountToPass;

    private final Locale locale;

    @Getter
    private final Map<String, String> fileNameByLocaleTag;

    @Getter
    private final String localeBundlePath;


    public AppConfiguration(Locale locale,
                            Map<String, String> fileNameByLocaleTag,
                            int rightAnswersCountToPass,
                            String localeBundlePath) {
        this.locale = locale;
        this.fileNameByLocaleTag = fileNameByLocaleTag;
        this.rightAnswersCountToPass = rightAnswersCountToPass;
        this.localeBundlePath = localeBundlePath;

    }

    @Override
    public int getRightAnswersCountToPass() {
        return rightAnswersCountToPass;
    }

    @Override
    public String getTestFileName() {
        return getFileNameByLocaleTag().get(locale.toLanguageTag());
    }


    @Override
    public Locale getLocale() {
        return locale;
    }
}
