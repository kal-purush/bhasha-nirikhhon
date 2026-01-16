package app.models;
import java.util.*;
import java.io.*;
import app.models.PeteDb.*;

/**
 * Abstract class for models
 * @author Pete Schmidt
 * @version 0.1 [August 30, 2013]
 */
public abstract class AbstractModel implements DbRecord, Serializable
{
    /** Unique id for this document */
    protected String id;

    /**
     * Get collection of all records
     */
    public <T extends AbstractModel> List<T> getCollection()
    {
        return Db.loadCollection(this);
    }
    
    /**
     * Get filtered collection of records
     */
    public <T extends AbstractModel> List<T> getCollectionWhere(String attribute, String value)
    {
        return Db.loadCollectionWhere(this, attribute, value);
    }
    
    /**
     * Get the unique id for this record
     * @return      a unique string to identify this record
     */
    @Override
    public String getDbRecordId()
    {
        return this.id;
    }

    /**
     * Set the unique id for this record
     * @param dbRecordId the unique string to identify this record
     */
    @Override
    public void setDbRecordId(String dbRecordId)
    {
        this.id = dbRecordId;
    }

    /**
     * Save record
     */
    public void save()
    {
        Db.save(this);
    }

    /**
     * Load record
     */
    public <T extends AbstractModel> T load()
    {
        return Db.load(this);
    }

    /**
     * Load record by id
     */
    public <T extends AbstractModel> T load(String id)
    {
        this.id = id;
        return this.load();
    }
}