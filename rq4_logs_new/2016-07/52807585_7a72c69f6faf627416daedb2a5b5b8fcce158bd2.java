package org.nrg.execution.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hibernate.SessionFactory;
import org.mockito.Mockito;
import org.nrg.execution.api.ContainerControlApi;
import org.nrg.execution.api.DockerControlApi;
import org.nrg.execution.daos.CommandDao;
import org.nrg.execution.model.Command;
import org.nrg.execution.model.DockerServerPrefsBean;
import org.nrg.execution.rest.DockerRestApi;
import org.nrg.execution.services.CommandService;
import org.nrg.execution.services.DockerHubService;
import org.nrg.execution.services.DockerService;
import org.nrg.execution.services.DockerServiceImpl;
import org.nrg.execution.services.HibernateCommandService;
import org.nrg.prefs.services.NrgPreferenceService;
import org.nrg.xdat.preferences.SiteConfigPreferences;
import org.nrg.xdat.services.AliasTokenService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.orm.hibernate4.HibernateTransactionManager;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
@EnableWebMvc
@Import(ExecutionHibernateEntityTestConfig.class)
public class DockerRestApiTestConfig {
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public DockerRestApi dockerRestApi() {
        return new DockerRestApi();
    }

    @Bean
    public DockerService dockerService() {
        return new DockerServiceImpl();
    }

    @Bean
    public DockerHubService mockDockerHubService() {
        return Mockito.mock(DockerHubService.class);
    }

    @Bean
    public DockerServerPrefsBean mockDockerServerPrefsBean() {
        return Mockito.mock(DockerServerPrefsBean.class);
    }

    @Bean
    public ContainerControlApi mockContainerControlApi() {
        return Mockito.mock(DockerControlApi.class);
    }

    @Bean
    public AliasTokenService aliasTokenService() {
        return Mockito.mock(AliasTokenService.class);
    }

    @Bean
    public SiteConfigPreferences siteConfigPreferences() {
        return Mockito.mock(SiteConfigPreferences.class);
    }

    @Bean
    public NrgPreferenceService nrgPreferenceService() {
        return Mockito.mock(NrgPreferenceService.class);
    }

    @Bean
    public CommandService commandService() {
        return new HibernateCommandService();
    }

    @Bean
    public CommandDao commandDao() {
        return new CommandDao();
    }

    @Bean
    public LocalSessionFactoryBean sessionFactory(final DataSource dataSource, @Qualifier("hibernateProperties") final Properties properties) {
        final LocalSessionFactoryBean bean = new LocalSessionFactoryBean();
        bean.setDataSource(dataSource);
        bean.setHibernateProperties(properties);
        bean.setAnnotatedClasses(
                Command.class);
        return bean;
    }

    @Bean
    public ResourceTransactionManager transactionManager(final SessionFactory sessionFactory) throws Exception {
        return new HibernateTransactionManager(sessionFactory);
    }
}