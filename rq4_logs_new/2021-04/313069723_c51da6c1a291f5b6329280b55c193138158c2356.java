package us.jbec.lct.models;

/**
 * Model representing a type of zip archive
 */
public enum ZipType {
    BULK("Bulk");

    private String description;

    ZipType(String directoryName) {
        this.description = directoryName;
    }

    public String getDescription() {
        return description;
    }
}