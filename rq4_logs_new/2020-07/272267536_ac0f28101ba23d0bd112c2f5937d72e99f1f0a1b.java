package br.lassal.dbvcs.tatubola.versioncontrol;

public class VersionControlSystemException extends Exception {
    public enum VersionControlSystemType {
        GIT
    }

    public static VersionControlSystemException createGitException(Throwable sourceException, String message) {
        if (message == null) {
            return new VersionControlSystemException(VersionControlSystemType.GIT, sourceException);
        } else {
            return new VersionControlSystemException(VersionControlSystemType.GIT, message, sourceException);
        }
    }

    private final VersionControlSystemType type;

    public VersionControlSystemException(VersionControlSystemType type, String message, Throwable sourceException) {
        super(message, sourceException);
        this.type = type;
    }

    public VersionControlSystemException(VersionControlSystemType type, Throwable sourceException) {
        super(sourceException);
        this.type = type;
    }

    public String getVCSType() {
        return this.type.name();
    }
}