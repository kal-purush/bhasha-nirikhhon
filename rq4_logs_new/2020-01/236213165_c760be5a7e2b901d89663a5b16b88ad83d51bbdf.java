package filegenerator.ast.model;

import filegenerator.filegenerator.StringUtils;

import java.io.IOException;

/**
 *
 * @author TheSadlig
 */
public class Chunk {

    private String chunkName;
    private String chunkOutputFolder;

    private String chunkContent;

    public Chunk(String chunkName, String chunkOutputFile, String chunkContent) {
        this.chunkName = chunkName;
        this.chunkOutputFolder = chunkOutputFile;
        this.chunkContent = chunkContent;
    }

    public String getChunkName() {
        return chunkName;
    }

    public String getChunkOutputFolder() {
        return chunkOutputFolder;
    }

    public String getChunkContent() {
        return chunkContent;
    }

    public void writeChunk(String outputFolder, Integer idFile) throws IOException {
        if (chunkContent != null) {
            StringUtils.writeFile(outputFolder + chunkOutputFolder + idFile + chunkName, chunkContent);
        }
    }
}