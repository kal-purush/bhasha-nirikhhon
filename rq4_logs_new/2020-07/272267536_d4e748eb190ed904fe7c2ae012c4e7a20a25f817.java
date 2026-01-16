package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.metrics.SerializerCounter;

import java.util.concurrent.CountDownLatch;

public class EnvironmentInfo {

    private DBModelSerializerBuilder dbEnvironment;
    private CountDownLatch taskSerializerLatch;
    private SerializerCounter serializerCounter;

    public EnvironmentInfo(DBModelSerializerBuilder dbEnvironment, int numberOfTaskToSerialize){
        this.dbEnvironment = dbEnvironment;
        this.taskSerializerLatch = new CountDownLatch(numberOfTaskToSerialize);
        this.serializerCounter = new SerializerCounter(dbEnvironment.getEnvironmentName());
    }

    public CountDownLatch getTaskSerializerLatch(){
        return this.taskSerializerLatch;
    }

    public String getEnvName(){
        return this.dbEnvironment.getEnvironmentName();
    }

    public String getSourceFolder(){
        return this.dbEnvironment.getNormalizedEnvironmentName();
    }

    public String getDBJdbcUrl(){
        return this.dbEnvironment.getJdbcUrl();
    }

    public SerializerCounter getSerializerCounter(){
        return this.serializerCounter;
    }


}