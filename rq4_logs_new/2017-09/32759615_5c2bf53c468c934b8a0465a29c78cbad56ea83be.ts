import WhereFilter from "./filter/WhereFilter";

export default interface IModelRepository {

    /**
     * Return the number of records that match the optional "where" filter.
     *
     * @param: [where] WhereFilter
     *   Optional where filter, like { key: val, key2: {gt: 'val2'}, ...}
     * @param: modelName string
     *   The database table/record to be queried.
     */
    count(where: WhereFilter, modelName: string);
    
    /**
     * Create new instance of Model, and save to database.
     * 
     * @param: {Object}|[{Object}]		
     *   data Optional data argument. Can be either a single model instance or an array of instances.
     */
    create(model);
    
    /**
     * Destroy all model instances that match the optional where specification.
     * 
     * @param: [where]	WhereFilter	
     *   Optional where filter, like: {key: val, key2: {gt: 'val2'}, ...} 
     * @param: modelName string
     *   the name of the table/record to be deleted.
     */
    destroyAll(where: WhereFilter, modelName: string);
    
    /**
     * Destroy model instance with the specified ID.
     * 
     * @param: id		
     *   The ID value of model instance to delete.
     * @param: [where]	WhereFilter	
     *   Optional where filter, like: {key: val, key2: {gt: 'val2'}, ...} 
     * @param: modelName string
     *   the name of the table/record to be deleted.
     */
    destroyById(id, where: WhereFilter, modelName: string);
    
    /**
     * Check whether a model instance exists in database.
     * 
     * @param: id	
     *   Identifier of object (primary key value).
     * @param: modelName string
     *   the name of the table/record to be deleted.
     */
    exists(id, modelName: string);
    
    /**
     * Find all model instances that match filter specification.
     *
     * @param: [where] WhereFilter
     *     Model instances matching the filter, or null if none found.
     * @param: modelName string
     *   the name of the table/record to be deleted.
     */
    find(where: WhereFilter, modelName: string);
    
    /**
     * Find object by ID with an optional filter for include/fields.
     * 
     * @param: id		
     *   Primary key value
     * @param: [where]	WhereFilter	
     *   Optional Filter JSON object
     */
    findById(id, where: WhereFilter);
    
    /**
     * Update single model instance that match the where clause.
     * 
     * @param: id 
     *   Primary key value
     * @param: [where] WhereFilter
     *   Optional where filter, like {}
     */
    updateById(id, where: WhereFilter);

    /**
     * Update multiple instances that match the where clause.
     * 
     * @param: [where]	WhereFilter 
     *   Optional where filter, like { key: val, key2: {gt: 'val2'}, ...} 
     *   see Where filter.
     * @param: data	Object	
     *   Object containing data to replace matching instances, if any.
     */
    updateAll(where: WhereFilter, data);

} 