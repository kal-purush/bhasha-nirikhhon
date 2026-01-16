package br.lassal.dbvcs.tatubola.fs;


import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;
import br.lassal.dbvcs.tatubola.text.TextSerializer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class InMemoryTestDBModelFS extends BaseDBModelFS{

    public static String getObjectID(DatabaseModelEntity object){
        return object.getSchema() + "." + object.getName();
    }

    public class SerializationInfo {
        private String objectId;
        private DatabaseModelEntity dbObject;
        private Path objectOutputPath;

        public SerializationInfo(DatabaseModelEntity dbObject, Path objectOutputPath){
            this.dbObject = dbObject;
            this.objectOutputPath = objectOutputPath;
            this.objectId = InMemoryTestDBModelFS.getObjectID(dbObject);
        }

        public String getObjectId(){
            return this.objectId;
        }

        public Path getObjectOutputPath(){
            return this.objectOutputPath;
        }

        public DatabaseModelEntity getDBObject(){
            return this.dbObject;
        }
    }

    private Map<String, SerializationInfo> serializedObjects;

    public InMemoryTestDBModelFS(String rootPath, TextSerializer textSerializer) {
        super(rootPath, textSerializer);
        this.serializedObjects = new HashMap<>();
    }

    @Override
    protected void safeWrite(Path dbModelFile, DatabaseModelEntity dbEntity) throws DBModelFSException, IOException {
        String objID = InMemoryTestDBModelFS.getObjectID(dbEntity);

        this.serializedObjects.put(objID, new SerializationInfo(dbEntity, dbModelFile));
    }

    public SerializationInfo getSerializationInfo(DatabaseModelEntity dbObject){
        String id = InMemoryTestDBModelFS.getObjectID(dbObject);

        return this.serializedObjects.get(id);
    }
}