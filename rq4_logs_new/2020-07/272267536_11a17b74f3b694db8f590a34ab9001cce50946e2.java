package br.lassal.dbvcs.tatubola.fs;

import br.lassal.dbvcs.tatubola.relationaldb.model.*;
import br.lassal.dbvcs.tatubola.text.TextSerializer;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/**
 * Create text file representation of the database object in YAML.
 * The files are organized and represented using the following
 * naming standard
 *
 *      - Tables
 *       - TABLE_{table name}.yaml
 *       - Triggers
 *         - TABLE_{table name}_TRIGGER_{trigger name}.yaml
 *       - Indexes
 *         - TABLE_{table name}_INDEX_{index name}.yaml
 *     - Routines
 *       - {name}_PROCEDURE.yaml
 *       - {name}_FUNCTION.yaml
 *
 */
public class BaseDBModelFS implements DBModelFS{

    private String rootPath;
    private TextSerializer objToTxtTranslator;
    private Set<Path> existingPaths;

    public BaseDBModelFS(String rootPath, TextSerializer textSerializer){
        this.rootPath = rootPath;
        this.objToTxtTranslator = textSerializer;
        this.existingPaths = new HashSet<>();
    }

    public Path save(DatabaseModelEntity dbEntity) throws Exception {

        Path dbModelFile = null;

        if(dbEntity instanceof Table){
            dbModelFile = Paths.get(rootPath, this.getFileName((Table) dbEntity));
        }
        else if(dbEntity instanceof Routine){
            dbModelFile = Paths.get(rootPath, this.getFileName((Routine) dbEntity));
        }
        else if(dbEntity instanceof Trigger){
            dbModelFile = Paths.get(rootPath, this.getFileName((Trigger) dbEntity));
        }
        else if(dbEntity instanceof Index){
            dbModelFile = Paths.get(rootPath, this.getFileName((Index) dbEntity));
        }
        else if(dbEntity instanceof View){
            dbModelFile = Paths.get(rootPath, this.getFileName((View) dbEntity));
        }


        this.safeWrite(dbModelFile, dbEntity);

        return dbModelFile;
    }

    private void safeWrite(Path dbModelFile, DatabaseModelEntity dbEntity) throws Exception {

        if(dbModelFile == null){
            throw new Exception("Object path NULL. Have you defined the path for this type of DB object ?");
        }

        this.verifyPath(dbModelFile.getParent());

        try(FileWriter writer = new FileWriter(dbModelFile.toFile())){

            writer.write(this.objToTxtTranslator.toText(dbEntity));
        }
    }

    private boolean verifyPath(Path path) throws IOException {
        if(this.existingPaths.contains(path)){
            return true;
        }
        else{
            if(Files.exists(path)){
                this.existingPaths.add(path);
                return true;
            }
            else{
                Files.createDirectories(path);
                this.existingPaths.add(path);
                return false;
            }
        }
    }

    /*
     - Tables
      - TABLE_{table name}.yaml
      - Triggers
        - TABLE_{table name}_TRIGGER_{trigger name}.yaml
      - Indexes
        - TABLE_{table name}_INDEX_{index name}.yaml
    - Routines
      - {name}_PROCEDURE.yaml
      - {name}_FUNCTION.yaml

     */


    private String getFileName(Table table){
        return String.format("%s/Tables/TABLE_%s.yaml", table.getSchema().toUpperCase(), table.getName());
    }

    private String getFileName(Trigger trigger){
        String objectPrefix = trigger.getTargetObjectType() != null
                ? trigger.getTargetObjectType().toUpperCase() : "OBJECT";

        return String.format("%s/Tables/Triggers/%s_%s_TRIGGER_%s.yaml", trigger.getTargetObjectSchema().toUpperCase()
                ,objectPrefix, trigger.getTargetObjectName(), trigger.getName());
    }

    private String getFileName(Routine routine){
        return String.format("%s/Routines/%s_%s.yaml", routine.getSchema().toUpperCase()
                ,routine.getName(), routine.getRoutineType());
    }

    private String getFileName(Index index){
        return String.format("%s/Tables/Indexes/TABLE_%s_INDEX_%s.yaml", index.getAssociateTableSchema().toUpperCase()
                , index.getAssociateTableName(), index.getName());
    }

    private String getFileName(View view){
        return String.format("%s/Views/VIEW_%s.yaml", view.getSchema().toUpperCase(), view.getName());
    }

}