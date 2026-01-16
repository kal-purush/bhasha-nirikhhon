package com.github.vincentrussell;


import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.apache.maven.project.ProjectBuilder;
import org.apache.maven.repository.RepositorySystem;
import org.apache.maven.shared.transfer.artifact.resolve.ArtifactResolver;
import org.apache.maven.shared.transfer.artifact.resolve.ArtifactResolverException;
import org.apache.maven.shared.transfer.project.deploy.ProjectDeployer;

import java.io.File;
import java.io.IOException;

/**
 * Goal for jenkins-dependency get
 */
@Mojo( name = "get", requiresProject = false, threadSafe = true )
public class GetMojo extends AbstractMojo {
     /**
     * @parameter default-value="${project}"
     * @required
     * @readonly
     */
    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    private MavenProject project;

    @Parameter(defaultValue = "${localRepository}", readonly = true, required = true)
    private ArtifactRepository localRepository;


    /**
     * The server that stores the jenkins plugins.
     */
    @Parameter( property = "jenkinsPluginServerUrl", defaultValue = "https://updates.jenkins-ci.org/download/plugins")
    private String jenkinsPluginServerUrl = "https://updates.jenkins-ci.org/download/plugins";

    /**
     * The groupId of the artifact to download. Ignored if {@link #artifact} is used.
     */
    @Parameter( property = "groupId" )
    private String groupId;

    /**
     * The artifactId of the artifact to download. Ignored if {@link #artifact} is used.
     */
    @Parameter( property = "artifactId" )
    private String artifactId;

    /**
     * The version of the artifact to download. Ignored if {@link #artifact} is used.
     */
    @Parameter( property = "version" )
    private String version;


    /**
     * A string of the form groupId:artifactId:version[:packaging][:classifier].
     */
    @Parameter( property = "artifact" )
    private String artifact;

    /**
     * The directory where to download the plugins
     */
    @Parameter( property = "downloadDir", required = true)
    private File downloadDir;

    @Parameter( defaultValue = "${session}", readonly = true, required = true )
    private MavenSession session;


    @Component
    private RepositorySystem repositorySystem;


    @Component
    private ArtifactResolver artifactResolver;

    /**
     * Used for attaching the artifacts to deploy to the project.
     */
    @Component
    private MavenProjectHelper projectHelper;

    /**
     * Used for creating the project to which the artifacts to deploy will be attached.
     */
    @Component
    private ProjectBuilder projectBuilder;

    /**
     * Component used to deploy project.
     */
    @Component
    private ProjectDeployer projectDeployer;

    public void execute() throws MojoExecutionException {
        JenkinsPluginGetter jenkinsPluginGetter = null;
        try {
            jenkinsPluginGetter = new JenkinsPluginGetter.Builder()
                    .setGroupId(groupId)
                    .setArtifactId(artifactId)
                    .setVersion(version)
                    .setArtifact(artifact)
                    .setDownloadDir(downloadDir)
                    .setJenkinsPluginServerUrl(jenkinsPluginServerUrl)
                    .setProjectDeployer(projectDeployer)
                    .setProjectBuilder(projectBuilder)
                    .setMavenSession(session)
                    .setProjectHelper(projectHelper)
                    .setArtifactResolver(artifactResolver)
                    .setRepositorySystem(repositorySystem)
                    .setLogger(getLog())
                    .build();
        } catch (MojoFailureException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }

        try {
            jenkinsPluginGetter.execute();
        } catch (IOException | ArtifactResolverException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }

}