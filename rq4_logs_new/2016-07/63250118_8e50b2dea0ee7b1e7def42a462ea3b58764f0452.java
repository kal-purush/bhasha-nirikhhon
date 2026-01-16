package enums;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.*;


/**
 * Created by Tbaios on 14.07.2016.
 */
public enum SingleInstanceChecker {

    INSTANCE; // HAHA! The CONFUSION!


    final public static int POLLINTERVAL = 1000;
    final public static File LOCKFILE = new File("SINGLE_INSTANCE_LOCKFILE");
    final public static File DETECTFILE = new File("EXTRA_INSTANCE_DETECTFILE");


    private boolean hasBeenUsedAlready = false;


    private WatchService watchService = null;
    private RandomAccessFile randomAccessFileForLock = null;
    private FileLock fileLock = null;


    /**
     * CAN ONLY BE CALLED ONCE.
     * <p>
     * Assumes that the program will close if FALSE is returned: The other-instance-tries-to-launch listener is not
     * installed in that case.
     * <p>
     * Checks if another instance is already running (temp file lock / shutdownhook). Depending on the accessibility of
     * the temp file the return value will be true or false. This approach even works even if the virtual machine
     * process gets killed. On the next run, the program can even detect if it has shut down irregularly, because then
     * the file will still exist. (Thanks to Robert http://stackoverflow.com/a/2002948/3500521 for that solution!)
     * <p>
     * Additionally, the method checks if another instance tries to start. In a crappy way, because as awesome as Java
     * is, it lacks some fundamental features. Don't worry, it has only been 25 years, it'll sure come eventually.
     *
     * @param codeToRunIfOtherInstanceTriesToStart Can be null. If not null and another instance tries to start (which
     *                                             changes the detect-file), the code will be executed. Could be used to
     *                                             bring the current (=old=only) instance to front. If null, then the
     *                                             watcher will not be installed at all, nor will the trigger file be
     *                                             created. (Null means that you just don't want to make use of this
     *                                             half of the class' purpose, but then you would be better advised to
     *                                             just use the 24 line method by Robert.)
     *                                             <p>
     *                                             BE CAREFUL with the code: It will potentially be called until the
     *                                             very last moment of the program's existence, so if you e.g. have a
     *                                             shutdown procedure or a window that would be brought to front, check
     *                                             if the procedure has not been triggered yet or if the window still
     *                                             exists / hasn't been disposed of yet. Or edit this class to be more
     *                                             comfortable. This would e.g. allow you to remove some crappy
     *                                             comments. Attribution would be nice, though.
     * @param executeOnAWTEventDispatchThread      Convenience function. If false, the code will just be executed. If
     *                                             true, it will be detected if we're currently on that thread. If so,
     *                                             the code will just be executed. If not so, the code will be run via
     *                                             SwingUtilities.invokeLater().
     * @return if this is the only instance
     */
    public boolean isOnlyInstance(final Runnable codeToRunIfOtherInstanceTriesToStart, final boolean executeOnAWTEventDispatchThread) {

        if (hasBeenUsedAlready) {
            throw new IllegalStateException("This class/method can only be used once, which kinda makes sense if you think about it.");
        }
        hasBeenUsedAlready = true;

        final boolean ret = canLockFileBeCreatedAndLocked();

        if (codeToRunIfOtherInstanceTriesToStart != null) {
            if (ret) {
                // Only if this is the only instance, it makes sense to install a watcher for additional instances.
                installOtherInstanceLaunchAttemptWatcher(codeToRunIfOtherInstanceTriesToStart, executeOnAWTEventDispatchThread);
            } else {
                // Only if this is NOT the only instance, it makes sense to create&delete the trigger file that will effect notification of the other instance.
                //
                // Regarding "codeToRunIfOtherInstanceTriesToStart != null":
                // While creation/deletion of the file concerns THE OTHER instance of the program,
                // making it dependent on the call made in THIS instance makes sense
                // because the code executed is probably the same.
                createAndDeleteOtherInstanceWatcherTriggerFile();
            }
        }

        optionallyInstallShutdownHookThatCleansEverythingUp();

        return ret;
    }


    private void createAndDeleteOtherInstanceWatcherTriggerFile() {

        try {
            final RandomAccessFile randomAccessFileForDetection = new RandomAccessFile(DETECTFILE, "rw");
            randomAccessFileForDetection.close();
            Files.deleteIfExists(DETECTFILE.toPath()); // File is created and then instantly deleted. Not a problem for the WatchService :)
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private boolean canLockFileBeCreatedAndLocked() {

        try {
            randomAccessFileForLock = new RandomAccessFile(LOCKFILE, "rw");
            fileLock = randomAccessFileForLock.getChannel().tryLock();
            return fileLock != null;
        } catch (Exception e) {
            return false;
        }
    }


    private void installOtherInstanceLaunchAttemptWatcher(final Runnable codeToRunIfOtherInstanceTriesToStart, final boolean executeOnAWTEventDispatchThread) {

        // PREPARE WATCHSERVICE AND STUFF
        try {
            watchService = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        final File appFolder = new File("").getAbsoluteFile(); // points to current folder
        final Path appFolderWatchable = appFolder.toPath();


        // REGISTER CURRENT FOLDER FOR WATCHING FOR FILE DELETIONS
        try {
            appFolderWatchable.register(watchService, StandardWatchEventKinds.ENTRY_DELETE);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }


        // INSTALL WATCHER THAT LOOKS IF OUR detectFile SHOWS UP IN THE DIRECTORY CHANGES. IF THERE'S A CHANGE, ANOTHER INSTANCE TRIED TO START, SO NOTIFY THE CURRENT ONE OF THAT.
        final Thread t = new Thread(() -> watchForDirectoryChangesOnExtraThread(codeToRunIfOtherInstanceTriesToStart, executeOnAWTEventDispatchThread));
        t.setDaemon(true);
        t.setName("directory content change watcher");
        t.start();
    }


    private void optionallyInstallShutdownHookThatCleansEverythingUp() {

        if (fileLock == null && randomAccessFileForLock == null && watchService == null) {
            return;
        }

        final Thread shutdownHookThread = new Thread(() -> {
            try {
                if (fileLock != null) {
                    fileLock.release();
                }
                if (randomAccessFileForLock != null) {
                    randomAccessFileForLock.close();
                }
                Files.deleteIfExists(LOCKFILE.toPath());
            } catch (Exception ignore) {
            }
            if (watchService != null) {
                try {
                    watchService.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHookThread);
    }


    private void watchForDirectoryChangesOnExtraThread(final Runnable codeToRunIfOtherInstanceTriesToStart, final boolean executeOnAWTEventDispatchThread) {

        while (true) { // To eternity and beyond! Until the universe shuts down. (Should be a volatile boolean, but this class only has absolutely required features.)

            try {
                Thread.sleep(POLLINTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            final WatchKey wk;
            try {
                wk = watchService.poll();
            } catch (ClosedWatchServiceException e) {
                // This situation would be normal if the watcher has been closed, but our application never does that.
                e.printStackTrace();
                return;
            }

            if (wk == null || !wk.isValid()) {
                continue;
            }


            for (WatchEvent<?> we : wk.pollEvents()) {

                final WatchEvent.Kind<?> kind = we.kind();
                if (kind == StandardWatchEventKinds.OVERFLOW) {
                    System.err.println("OVERFLOW of directory change events!");
                    continue;
                }


                final WatchEvent<Path> watchEvent = (WatchEvent<Path>) we;
                final File file = watchEvent.context().toFile();


                if (file.equals(DETECTFILE)) {

                    if (!executeOnAWTEventDispatchThread || SwingUtilities.isEventDispatchThread()) {
                        codeToRunIfOtherInstanceTriesToStart.run();
                    } else {
                        SwingUtilities.invokeLater(codeToRunIfOtherInstanceTriesToStart);
                    }

                    break;

                } else {
                    System.err.println("THIS IS THE FILE THAT WAS DELETED: " + file);
                }

            }

            wk.reset();
        }
    }

}