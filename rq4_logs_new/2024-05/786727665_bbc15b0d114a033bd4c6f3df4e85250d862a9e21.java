package com.madr.external_dictionaries.monctionary;

import com.madr.external_dictionaries.mongomodel.protobuf.Common;
import java.io.File;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "spring.application")
@Getter
@Setter
public class DictionariesConfiguration {
    private List<Dictionary> dictionaries;

    public record Dictionary(Common.SupportedLanguage wiktionaryLanguage, Common.SupportedLanguage wordsLanguage,
                              File dictionary) {
    }
}