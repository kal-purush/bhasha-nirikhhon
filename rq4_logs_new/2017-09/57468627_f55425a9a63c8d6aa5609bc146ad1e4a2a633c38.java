package org.jtwig.xliff.i18n;

import org.jtwig.environment.EnvironmentConfigurationBuilder;
import org.jtwig.environment.EnvironmentFactory;
import org.jtwig.translate.message.source.MessageSource;
import org.jtwig.translate.message.source.factory.MessageSourceFactory;
import org.junit.Test;

import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

public class XliffMessageSourceFactoryBuilderTest {
    @Test
    public void builder() throws Exception {
        MessageSourceFactory factory = new XliffMessageSourceFactoryBuilder(XmlFileFilter.xmlFileFilter())
                .withLookupClasspath("test")
                .withLookupClasspathRecursively("test")
                .withLookupDirectory("test")
                .withLookupDirectoryRecursively("test")
                .build();

        MessageSource result = factory.create(new EnvironmentFactory().create(EnvironmentConfigurationBuilder.configuration().build()));

        assertThat(result, notNullValue());
    }
}