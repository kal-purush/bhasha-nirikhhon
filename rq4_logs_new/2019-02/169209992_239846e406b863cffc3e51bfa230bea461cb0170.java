package org.elaastic.qtapi.entities;

import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.validation.constraints.NotNull;

@Entity
public class Attachement {

    private static final String[] TYPES_MIME_IMG_DISPLAYABLE = {"image/gif","image/jpeg","image/png"};

    @NotNull
    private String path;
    @NotNull
    private String name;
    private String originalName;
    private Integer size;
    @Embedded
    private Dimension dimension;
    private String typeMime;

    private Statement statement;
    private Boolean toDelete = false;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOriginalName() {
        return originalName;
    }

    public void setOriginalName(String originalName) {
        this.originalName = originalName;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Dimension getDimension() {
        return dimension;
    }

    public void setDimension(Dimension dimension) {
        this.dimension = dimension;
    }

    public String getTypeMime() {
        return typeMime;
    }

    public void setTypeMime(String typeMime) {
        this.typeMime = typeMime;
    }

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public Boolean getToDelete() {
        return toDelete;
    }

    public void setToDelete(Boolean toDelete) {
        this.toDelete = toDelete;
    }
}

class Dimension implements Comparable<Dimension> {
    Integer width;
    Integer height;

    public String toString() {
        return "dim    h: " + height + "      l: " + width;
    }

    @Override
    public int compareTo(Dimension other) {
        if (width == other.width && height == other.height) {
            return 0;
        }

        if (width > other.width || height > other.height) {
            return 1;
        }

        return -1;
    }
}