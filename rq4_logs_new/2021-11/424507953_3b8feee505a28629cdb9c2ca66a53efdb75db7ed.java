package com.hazelcast.maven.attribution;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.DefaultProjectBuildingRequest;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.ProjectBuildingRequest;
import org.apache.maven.shared.transfer.artifact.resolve.ArtifactResolverException;

public abstract class AbstractAttributionMojo extends AbstractMojo {

    public static final String DEFAULT_COPYRIGHT_PATTERN = "(?i)^([\\s/*]*)(((\\(c\\))|(copyright))\\s+\\S[^;{}]*)$";
    public static final int DEFAULT_COPYRIGHT_PATTERN_GRPIDX = 2;

    /**
     * The classifier for sources.
     */
    public static final String SOURCES_CLASSIFIER = "sources";

    @Component
    private ResolverComponent resolverComponent;

    @Parameter(defaultValue = "${session}", readonly = true, required = true)
    protected MavenSession session;

    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    protected MavenProject project;

    @Parameter(property = "reactorProjects", readonly = true)
    protected List<MavenProject> reactorProjects;

    /**
     * Specifies whether the attribution file generation should be skipped.
     */
    @Parameter(property = "attribution.skip", defaultValue = "false")
    protected boolean skip;

    /**
     * Configures the parallelism level. By default the number of available processors is used. Parallelism says (1) how many
     * new threads is used for reading source archives and also (2) how many new threads is used for search the copyright
     * notices.
     */
    @Parameter(property = "attribution.parallelism", defaultValue = "0")
    protected int parallelism;

    /**
     * Customizes the pattern for finding the "attribution lines".
     *
     * @see #copyrightPatternGroupIndex
     * @see #DEFAULT_COPYRIGHT_PATTERN
     */
    @Parameter(property = "attribution.copyrightPattern", defaultValue = "")
    protected String copyrightPattern;

    /**
     * When the {@link #copyrightPattern} is configured, then this parameter allows to specify which capture group is used. By
     * default the whole pattern is used (group==0) when custom pattern is configured.
     * 
     * @see #copyrightPattern
     * @see #DEFAULT_COPYRIGHT_PATTERN_GRPIDX
     */
    @Parameter(property = "attribution.copyrightPatternGroupIndex", defaultValue = "0")
    protected int copyrightPatternGroupIndex;

    /**
     * Maximal wait time for finishing reading source JARs and searching for patterns in the found source files.
     */
    @Parameter(property = "attribution.serviceTimeoutMinutes", defaultValue = "60")
    private int serviceTimeoutMinutes;

    /**
     * Specifies the destination attribution file.
     */
    @Parameter(property = "attribution.outputFile", defaultValue = "${project.build.directory}/attribution.txt", required = true)
    protected File outputFile;

    public void execute() throws MojoExecutionException, MojoFailureException {
        if (skip) {
            getLog().info("Skipping the plugin execution");
        }

        if (outputFile == null) {
            throw new MojoFailureException("The outputFile has to be configured");
        }

        if (outputFile.exists()) {
            try {
                FileUtils.forceDelete(outputFile);
            } catch (IOException e) {
                throw new MojoExecutionException("Unable to remove the outputFile " + outputFile, e);
            }
        }

        Map<String, File> sourceJars = resolveSourceJars();

        int threads = parallelism > 0 ? parallelism : Runtime.getRuntime().availableProcessors();
        ExecutorService jarReaderService = Executors.newFixedThreadPool(threads);
        final AttributionContext context = new AttributionContext();
        for (Map.Entry<String, File> entry : sourceJars.entrySet()) {
            jarReaderService.submit(() -> readJar(entry.getKey(), entry.getValue(), context));
        }
        ExecutorService consumerExecutorService = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            consumerExecutorService.submit(() -> consumeSrc(context));
        }

        consumerExecutorService.shutdown();
        jarReaderService.shutdown();
        try {
            jarReaderService.awaitTermination(serviceTimeoutMinutes, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            getLog().error(e);
            throw new MojoFailureException("JAR files processing has timed out", e);
        }
        context.producersRunning.set(false);
        try {
            consumerExecutorService.awaitTermination(serviceTimeoutMinutes, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            getLog().error(e);
            throw new MojoFailureException("Source files processing has timed out", e);
        }

        generateResults(context);
    }

    private void generateResults(final AttributionContext context) throws MojoExecutionException {
        File parentDir = outputFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        try (PrintWriter pw = new PrintWriter(
                new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(outputFile)), StandardCharsets.UTF_8))) {
            for (Map.Entry<String, Set<String>> gavEntry : context.foundAttribution.entrySet()) {
                Set<String> attributionSet = gavEntry.getValue();
                String gav = gavEntry.getKey();
                if (!attributionSet.isEmpty()) {
                    pw.println(gav);
                    getLog().debug("Adding " + attributionSet.size() + " attribution(s) for " + gav);
                    for (String attribution : attributionSet) {
                        String attributionLine = "\t" + attribution;
                        pw.println(attributionLine);
                        getLog().debug(attributionLine);
                    }
                    pw.println();
                } else {
                    getLog().info("Skipping " + gav + " as no attribution was found there.");
                }
            }
        } catch (FileNotFoundException e) {
            throw new MojoExecutionException("Unable to write to the outputFile " + outputFile, e);
        }
    }

    protected abstract Map<String, File> resolveSourceJars();

    private void readJar(String gav, File jar, AttributionContext context) {
        try (ZipInputStream zip = new ZipInputStream(new BufferedInputStream(new FileInputStream(jar)))) {
            ZipEntry zipEntry;
            while (null != (zipEntry = zip.getNextEntry())) {
                String srcName = zipEntry.getName();
                if (!zipEntry.isDirectory() && acceptFile(srcName)) {
                    try {
                        context.srcQueue.put(new SrcFile(gav, srcName, toByteArray(zip)));
                    } catch (InterruptedException e) {
                        getLog().warn("Putting source file to queue was interrupted", e);
                    } catch (IOException e) {
                        getLog().warn("Reading source file failed", e);
                    }
                }
                zip.closeEntry();
            }
        } catch (IOException e) {
            getLog().error("Reading archive failed: " + jar, e);
        }
    }

    private boolean acceptFile(String srcName) {
        String nameLowerCase = srcName.toLowerCase(Locale.ROOT);
        return nameLowerCase.endsWith(".java") || nameLowerCase.endsWith(".xml");
    }

    private void consumeSrc(AttributionContext context) {
        String patternStr = DEFAULT_COPYRIGHT_PATTERN;
        int group = DEFAULT_COPYRIGHT_PATTERN_GRPIDX;
        if (copyrightPattern != null && !copyrightPattern.isEmpty()) {
            patternStr = copyrightPattern;
            group = copyrightPatternGroupIndex >= 0 ? copyrightPatternGroupIndex : 0;
        }
        Pattern pattern = Pattern.compile(patternStr);
        while (context.producersRunning.get() || !context.srcQueue.isEmpty()) {
            try {
                SrcFile srcFile = context.srcQueue.poll(1, TimeUnit.SECONDS);
                getLog().debug("Processing " + srcFile.getSourceName() + " from " + srcFile.getGav());
                Set<String> hitSet = context.foundAttribution.computeIfAbsent(srcFile.getGav(),
                        s -> Collections.newSetFromMap(new ConcurrentSkipListMap<>()));
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(new ByteArrayInputStream(srcFile.getBytes()), StandardCharsets.UTF_8))) {
                    String line;
                    while (null != (line = reader.readLine())) {
                        Matcher m = pattern.matcher(line);
                        if (m.find()) {
                            String copyrightStr = m.group(group);
                            if (hitSet.add(copyrightStr)) {
                                getLog().debug("Found: " + copyrightStr);
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                getLog().debug(e);
            } catch (IOException e) {
                getLog().error(e);
            }
        }
    }

    private static byte[] toByteArray(InputStream in) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = in.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            return baos.toByteArray();
        }
    }

    protected File resolve(Artifact artifact) {
        if (!SOURCES_CLASSIFIER.equals(artifact.getClassifier())) {
            return null;
        }

        Artifact resolvedArtifact = null;
        try {
            resolvedArtifact = resolverComponent.getResolver().resolveArtifact(getProjectBuildingRequest(project), artifact)
                    .getArtifact();
            getLog().info(" > resolved " + resolvedArtifact.getFile());
        } catch (ArtifactResolverException e1) {
            getLog().info("Resolving failed for " + artifact);
        }
        return resolvedArtifact == null ? null : resolvedArtifact.getFile();
    }

    protected Artifact createResourceArtifact(final Artifact artifact, final String classifier) {
        final DefaultArtifact a = (DefaultArtifact) resolverComponent.getArtifactFactory().createArtifactWithClassifier(
                artifact.getGroupId(), artifact.getArtifactId(), artifact.getVersion(), "jar", classifier);

        a.setRepository(artifact.getRepository());

        return a;
    }

    private ProjectBuildingRequest getProjectBuildingRequest(MavenProject currentProject) {
        return new DefaultProjectBuildingRequest(session.getProjectBuildingRequest())
                .setRemoteRepositories(currentProject.getRemoteArtifactRepositories());
    }

    protected static String gaKey(Artifact artifact) {
        return gaKey(artifact.getGroupId(), artifact.getArtifactId());
    }

    protected static String gavKey(Artifact artifact) {
        return gavKey(artifact.getGroupId(), artifact.getArtifactId(), artifact.getVersion());
    }

    protected static String gaKey(String gid, String aid) {
        return gid + ":" + aid;
    }

    protected static String gavKey(String gid, String aid, String version) {
        return gaKey(gid, aid) + ":" + version;
    }

}