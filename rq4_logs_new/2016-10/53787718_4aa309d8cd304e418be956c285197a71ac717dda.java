package sp.sd.nexusartifactuploader;

import org.apache.maven.repository.internal.*;
import org.apache.maven.settings.Settings;
import org.apache.maven.settings.building.DefaultSettingsBuilderFactory;
import org.apache.maven.settings.building.DefaultSettingsBuildingRequest;
import org.apache.maven.settings.building.SettingsBuildingException;
import org.apache.maven.settings.building.SettingsBuildingRequest;
import org.sonatype.aether.RepositorySystem;
import org.sonatype.aether.RepositorySystemSession;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.connector.async.AsyncRepositoryConnectorFactory;
import org.sonatype.aether.connector.file.FileRepositoryConnectorFactory;
import org.sonatype.aether.deployment.DeployRequest;
import org.sonatype.aether.impl.ArtifactDescriptorReader;
import org.sonatype.aether.impl.MetadataGeneratorFactory;
import org.sonatype.aether.impl.VersionRangeResolver;
import org.sonatype.aether.impl.VersionResolver;
import org.sonatype.aether.impl.internal.DefaultServiceLocator;
import org.sonatype.aether.repository.*;
import org.sonatype.aether.spi.connector.RepositoryConnectorFactory;
import org.sonatype.aether.util.artifact.DefaultArtifact;

import java.io.File;

public class ArtifactRepositoryManager {

    private String url;
    private String username;
    private String password;
    private String repo;

    private static final String USER_HOME = System.getProperty("user.home");
    private static final File MAVEN_USER_HOME = new File(USER_HOME, ".m2");

    private final RepositorySystem repositorySystem;
    private final RepositorySystemSession session;

    public ArtifactRepositoryManager(String url, String username, String password, String repo)
            throws SettingsBuildingException {
        this.url = url;
        this.username = username;
        this.password = password;
        this.repo = repo;

        final SettingsBuildingRequest request = new DefaultSettingsBuildingRequest()
                .setSystemProperties(System.getProperties());

        Settings settings = new DefaultSettingsBuilderFactory()
                .newInstance()
                .build(request)
                .getEffectiveSettings();

        repositorySystem = new DefaultServiceLocator()
                .addService(RepositoryConnectorFactory.class, AsyncRepositoryConnectorFactory.class)
                .addService(RepositoryConnectorFactory.class, FileRepositoryConnectorFactory.class)
                .addService(VersionResolver.class, DefaultVersionResolver.class)
                .addService(VersionRangeResolver.class, DefaultVersionRangeResolver.class)
                .addService(ArtifactDescriptorReader.class, DefaultArtifactDescriptorReader.class)
                .addService(MetadataGeneratorFactory.class, SnapshotMetadataGeneratorFactory.class)
                .getService(RepositorySystem.class);

        String localRepository = settings.getLocalRepository();
        if (localRepository == null || localRepository.trim().isEmpty()) {
            localRepository = new File(MAVEN_USER_HOME, "repository").getAbsolutePath();
        }

        LocalRepository localRepositoryFromPath = new LocalRepository(localRepository);
        LocalRepositoryManager localRepositoryManager =
                repositorySystem.newLocalRepositoryManager(localRepositoryFromPath);

        session = new MavenRepositorySystemSession()
                .setLocalRepositoryManager(localRepositoryManager);

    }

    private RemoteRepository makeRemoteRepository() {
        return new RemoteRepository(this.repo, "default", this.url)
                .setAuthentication(new Authentication(this.username, this.password));
    }

    public void upload(String groupId, String artifactId, String version,
                       File artifactFile, String type, String classifier)
            throws Exception {
        Artifact artifact = new DefaultArtifact(groupId, artifactId, classifier,
                type, version).setFile(artifactFile);
        DeployRequest deployRequest = new DeployRequest().addArtifact(artifact);
        RemoteRepository remoteRepository = makeRemoteRepository();
        deployRequest.setRepository(remoteRepository);
        repositorySystem.deploy(session, deployRequest);
    }

}
