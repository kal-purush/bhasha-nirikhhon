package br.ufsc.lapesd.freqel.util;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ProcessUtils {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(ProcessUtils.class);
    private static final @Nonnull Map<File, MavenProject> mvnProjects = new ConcurrentHashMap<>();

    private static class MavenProject {
        private final @Nonnull Object lock = new Object();
        private final @Nonnull File dir;
        private @Nullable List<String> builtWithMavenArgs = null;

        public MavenProject(@Nonnull File dir) {
            this.dir = dir;
        }

        public @Nonnull Object getLock() {
            return lock;
        }
        public boolean isBuilt() {
            return builtWithMavenArgs != null;
        }
        public @Nullable File getJar(@Nonnull List<String> mvnArgs, @Nonnull String jarName) {
            if (!isBuilt())
                return null;
            File jar = new File(new File(dir, "target"), jarName);
            return jar.isFile() ? jar : null;
        }
        public void setBuiltWithMavenArgs(@Nonnull List<String> builtWithMavenArgs) {
            this.builtWithMavenArgs = builtWithMavenArgs;
        }
    }

    private static @Nonnull MavenProject getMavenProject(@Nonnull File dir) {
        try {
            dir = dir.getCanonicalFile();
        } catch (IOException ignored) {}
        return mvnProjects.computeIfAbsent(dir, MavenProject::new);
    }

    public static @Nonnull File getMvnWrapper(@Nonnull File projectDir,
                                              @Nullable String mvnWrapperRelDir)
            throws FileNotFoundException {
        mvnWrapperRelDir = mvnWrapperRelDir == null || mvnWrapperRelDir.isEmpty()
                ? ""
                : mvnWrapperRelDir + (mvnWrapperRelDir.endsWith("/") ? "" : "/");
        String mavenWrapperSuffix = SystemUtils.IS_OS_WINDOWS ? ".cmd" : "";
        String wrapperFilename = mvnWrapperRelDir + "mvnw" + mavenWrapperSuffix;
        File wrapperFile = new File(projectDir, wrapperFilename);
        if (!wrapperFile.isFile()) {
            throw new FileNotFoundException("No maven wrapper "+wrapperFilename+
                    " in "+ projectDir.getAbsolutePath());
        }
        try {
            return wrapperFile.getAbsoluteFile().getCanonicalFile();
        } catch (IOException e) {
            throw new FileNotFoundException("IOException getting canonical path for "+
                                            wrapperFile.getAbsolutePath());
        }
    }

    @Nonnull private static File getProjectDir(@Nonnull String dirName) throws FileNotFoundException {
        File dir = new File(dirName);
        if (!dir.isDirectory()) {
            File dir2 = new File("../"+ dirName);
            if (!dir2.exists()) {
                throw new FileNotFoundException(dirName +"directory not found, " +
                        "tried "+dir.getAbsolutePath()+" and "+dir2.getAbsolutePath());
            }
            dir = dir2;
        }
        try {
            dir = dir.getCanonicalFile();
        } catch (IOException e) {
            throw new FileNotFoundException("Could not canonicalize "+dir);
        }
        return dir;
    }

    public static @Nonnull File mavenWrapperBuild(@Nonnull String dirName,
                                                  @Nullable String mavenWrapperRelDir,
                                                  @Nonnull String jarFilename,
                                                  @Nonnull String... args)
            throws IOException {
        File projectDir = getProjectDir(dirName);
        MavenProject mvnProject = getMavenProject(projectDir);
        synchronized (mvnProject.getLock()) {
            File jar = mvnProject.getJar(Arrays.asList(args), jarFilename);
            if (jar != null)
                return jar;
            File wrapper = getMvnWrapper(projectDir, mavenWrapperRelDir);
            List<String> command = new ArrayList<>(Arrays.asList(args));
            command.add(0, wrapper.getAbsolutePath());
            String commandString = String.join(" ", command);
            Process build = new ProcessBuilder()
                    .command(command)
                    .directory(projectDir)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .start();
            if (!ProcessUtils.waitProcessUninterruptibly(build, 4, TimeUnit.MINUTES)) {
                logger.error("Timeout while building " + dirName);
                stopProcess(build, commandString);
                throw new IOException("Maven wrapper at " + wrapper + " timed out");
            }
            if (build.exitValue() != 0)
                throw new IOException("Non-zero ("+build.exitValue()+") exit from "+commandString);
            jar = new File(new File(projectDir, "target"), jarFilename);
            if (!jar.isFile())
                throw new IOException("JAR file " + jar + " not found after build");
            mvnProject.setBuiltWithMavenArgs(Arrays.asList(args));
            return jar;
        }
    }

    public static void waitOrStopProcess(@Nonnull Process process, long timeout,
                                         @Nonnull TimeUnit timeUnit,
                                         @Nonnull String what) {
        if (!waitProcessUninterruptibly(process, timeout, timeUnit)) {
            logger.warn("Timeout on {} after {} {}", what, timeout, timeUnit);
            stopProcess(process, what);
        }
    }


    public static boolean waitProcessUninterruptibly(@Nonnull Process process, long timeout,
                                                     @Nonnull TimeUnit timeUnit) {
        boolean interrupted = false;
        long usTimeout = TimeUnit.MICROSECONDS.convert(timeout, timeUnit);
        Stopwatch sw = Stopwatch.createStarted();
        for (long rem = usTimeout; rem > 0; rem = usTimeout - sw.elapsed(TimeUnit.MICROSECONDS)) {
            try {
                if (process.waitFor(rem, TimeUnit.MICROSECONDS))
                    return true;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
        return false;
    }

    public static void stopProcess(@Nonnull Process process, @Nonnull String what) {
        stopProcess(process, what, 5, 5);
    }
    public static void stopProcess(@Nonnull Process process, @Nonnull String what,
                                   int termTimeoutSeconds, int killTimeoutSeconds)  {
        process.destroy();
        if (!waitProcessUninterruptibly(process, termTimeoutSeconds, TimeUnit.SECONDS)) {
            logger.error(what +" did not finish in 5s after SIGTERM, sending SIGKILL...");
            if (!waitProcessUninterruptibly(process, killTimeoutSeconds, TimeUnit.SECONDS))
                logger.error(what +" did not finish in 5s after SIGKILL. Will ignore");
        }
    }
}