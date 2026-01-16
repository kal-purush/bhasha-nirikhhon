package com.albertosoto.mgnl.rd2019.data;

/**
 * com.albertosoto.mgnl.rd2019.data
 * Class StreamingConfig
 * 24/05/2019
 *
 * @author berto (alberto.soto@gmail.com)
 */
public class StreamingConfig implements IdentifiedJCRItem {

    private Integer chunkSize;
    private String defaultFileItem;
    private String defaultJCRItem;
    private String id;


    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    public String getDefaultFileItem() {
        return defaultFileItem;
    }

    public void setDefaultFileItem(String defaultFileItem) {
        this.defaultFileItem = defaultFileItem;
    }

    public String getDefaultJCRItem() {
        return defaultJCRItem;
    }

    public void setDefaultJCRItem(String defaultJCRItem) {
        this.defaultJCRItem = defaultJCRItem;
    }

    public Integer getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(Integer chunkSize) {
        this.chunkSize = chunkSize;
    }
}