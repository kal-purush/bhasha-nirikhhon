package br.lassal.dbvcs.tatubola.builder;

import br.lassal.dbvcs.tatubola.ParallelDBVersionCommand;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builder class to create ParallelDBVersionCommand
 *
 * The command to run need to have access a VCS repository, a local workspace to serialize the files, and the DB
 * environments and schemas that need to be serialized.
 *
 * To create a new ParallelDBVersionCommand call the build() method after call all those methods
 *   - setVCSRemoteInfo
 *   - setWorkspaceInfo
 *   - setDBSchemasToBeSerialized
 *   - addDBEnvironment
 */
public class ParallelDBVersionCommandBuilder {

    private static Logger logger = LoggerFactory.getLogger(ParallelDBVersionCommandBuilder.class);

    private int parallelismLevel = ParallelDBVersionCommand.DEFAULT_PARALLELISM;
    private VersionControlSystem vcs;
    private String workspacePath;
    private String tmpPath;
    private List<String> schemas;
    private List<DBModelSerializerBuilder> dbEnvironments;

    public ParallelDBVersionCommandBuilder(int parallelismLevel){
        this.parallelismLevel = parallelismLevel;
        this.dbEnvironments = new ArrayList<>();
    }

    public ParallelDBVersionCommandBuilder setVCSRemoteInfo(String remoteUrl, String baseBranch
            , String username, String password) throws MalformedURLException {
        this.vcs = RelationalDBVersionFactory.getInstance()
                .createVCSController(remoteUrl, username, password, baseBranch);

        return this;
    }

    public ParallelDBVersionCommandBuilder setWorkspaceInfo(String workspacePath, String tmpPath){
        this.workspacePath = workspacePath;
        this.tmpPath = tmpPath;

        return this;
    }

    public ParallelDBVersionCommandBuilder setDBSchemasToBeSerialized(String ...schemas){
        if(schemas != null && schemas.length > 0){
            this.schemas = Arrays.asList(schemas);
        }

        return this;
    }

    public ParallelDBVersionCommandBuilder setDBSchemasToBeSerialized(List<String> schemas){
        this.schemas = schemas;

        return this;
    }

    public ParallelDBVersionCommandBuilder addDBEnvironment(String envName, String jdbcUrl, String username, String password){
        this.dbEnvironments
                .add(new DBModelSerializerBuilder(envName, jdbcUrl, username, password));

        return this;
    }

    /**
     * Create a new instance ParallelDBVersionCommand using the information provided previously
     *
     * @return new ParallelDBVersionCommand instance
     */
    public ParallelDBVersionCommand build(){
        if(this.vcs == null){
            logger.error("Error building ParallelDBVersionCommand : please setVCSRemoteInfo");
            return null;
        }

        if(this.workspacePath == null || this.workspacePath.isEmpty() || this.tmpPath == null || this.tmpPath.isEmpty()){
            logger.error("Error building ParallelDBVersionCommand : please setWorkspaceInfo");
            return null;
        }

        if(this.dbEnvironments.isEmpty()){
            logger.error("Error building ParallelDBVersionCommand : please inform at least 1 DB environment to be serialized addDBEnvironment");
            return null;
        }

        ParallelDBVersionCommand command = new ParallelDBVersionCommand(this.schemas, this.workspacePath, this.tmpPath, this.vcs, this.parallelismLevel);

        for(DBModelSerializerBuilder dbEnv : this.dbEnvironments){
            command.addDBEnvironment(dbEnv);
        }

        return command;
    }

}