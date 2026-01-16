package ir.visoft.accounting.db;

import ir.visoft.accounting.annotation.EntityField;
import ir.visoft.accounting.annotation.PK;
import ir.visoft.accounting.entity.BaseEntity;
import ir.visoft.accounting.exception.DatabaseOperationException;
import ir.visoft.accounting.exception.DeveloperFaultException;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Amir
 */
public class DatabaseUtil {

    private static Logger log = Logger.getLogger(DatabaseUtil.class.getName());


    public static <T> List<T> getEntity(BaseEntity entity) throws DatabaseOperationException {
        String query = "SELECT * FROM " + entity.getClass().getSimpleName() + " WHERE ";

        Set<Field> entityFields = findFields(entity.getClass(), EntityField.class);
        for (Field entityField : entityFields) {

            Method getter = null;
            try {
                getter = entity.getClass().getMethod(getGetterMethodName(entityField.getName()));
                Object value = getter.invoke(entity);
                if(value != null && !value.toString().equals("")) {
                    query += getDBFieldName(entityField.getName()) + "='" + getter.invoke(entity) + "' AND ";
                }
            } catch (NoSuchMethodException e) {
                log.error("Problem in database querying...");
                log.error(e.getMessage());
                throw new DatabaseOperationException();
            } catch (InvocationTargetException e) {
                log.error("Problem in database querying...");
                log.error(e.getMessage());
                throw new DatabaseOperationException();
            } catch (IllegalAccessException e) {
                log.error("Problem in database querying...");
                log.error(e.getMessage());
                throw new DatabaseOperationException();
            }
        }
        query = query.substring(0, query.length() - 4);

        return getResult(query, entity);
    }


    public static <T> List<T> getAll(BaseEntity entity) throws DatabaseOperationException {
        String query = "SELECT * FROM " + entity.getClass().getSimpleName();
        return getResult(query, entity);
    }


    public static Set<Field> findFields(Class<?> clazz, Class<? extends Annotation> annotation) {
        Set<Field> set = new HashSet<Field>();
        Class<?> c = clazz;
        while (c != null) {
            for (Field field : c.getDeclaredFields()) {
                if (field.isAnnotationPresent(annotation)) {
                    set.add(field);
                }
            }
            c = c.getSuperclass();
        }
        return set;
    }


    public static <T> List<T> getResult(String query, BaseEntity entity) throws DatabaseOperationException {
        List<Map<String, Object>> rawResults = null;
        List<T> results = new ArrayList<T>();
        try {
            rawResults = runQuery(query, DbOperation.SELECT);
            for (Map<String, Object> rawResult : rawResults) {
                T result = (T)entity.getClass().getConstructor().newInstance();
                for (String columnName : rawResult.keySet()) {
                    String fieldName = getEntityFieldName(columnName);
                    Method setter = entity.getClass().getMethod(getSetterMethodName(fieldName),
                            entity.getClass().getDeclaredField(fieldName).getType());
                    Object value = rawResult.get(columnName);
                    setter.invoke(result, value);
                }
                results.add(result);
            }

        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        } catch (NoSuchMethodException e) {
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        } catch (InstantiationException e) {
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        } catch (IllegalAccessException e) {
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        } catch (InvocationTargetException e) {
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        } catch (NoSuchFieldException e) {
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        }
        return results;
    }


    public static <T extends BaseEntity> void update(T entity) throws DatabaseOperationException {
        String query = "UPDATE " + entity.getClass().getSimpleName() + " SET ";

        try {
            Set<Field> entityFields = findFields(entity.getClass(), EntityField.class);
            for (Field entityField : entityFields) {
                Method getter = null;

                    getter = entity.getClass().getMethod(getGetterMethodName(entityField.getName()));
                    Object value = getter.invoke(entity);
                    if(value != null && !value.toString().equals("")) {
                        query += getDBFieldName(entityField.getName()) + "='" + getter.invoke(entity) + "', ";
                    }
            }
            query = query.substring(0, query.length() - 2);

            Set<Field> primaryKeyFields = findFields(entity.getClass(), PK.class);
            if(primaryKeyFields != null && !primaryKeyFields.isEmpty() && primaryKeyFields.size() == 1) {
                for (Field primaryKeyField : primaryKeyFields) {
                    Method getter = entity.getClass().getMethod(getGetterMethodName(primaryKeyField.getName()));
                    query += " WHERE " + getDBFieldName(primaryKeyField.getName()) + " = '" + getter.invoke(entity) + "' ";
                    break;
                }
            }

        } catch (NoSuchMethodException e) {
            log.error("Problem in database querying...");
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        } catch (InvocationTargetException e) {
            log.error("Problem in database querying...");
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        } catch (IllegalAccessException e) {
            log.error("Problem in database querying...");
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        }

        try {
            runQuery(query, DbOperation.UPDATE);
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        }
    }


    public static <T extends BaseEntity> void delete(T entity) throws DatabaseOperationException {
        String query = "DELETE FROM " + entity.getClass().getSimpleName() + " WHERE ";

        Set<Field> entityFields = findFields(entity.getClass(), EntityField.class);
        for (Field entityField : entityFields) {

            Method getter = null;
            try {
                getter = entity.getClass().getMethod(getGetterMethodName(entityField.getName()));
                Object value = getter.invoke(entity);
                if(value != null && !value.toString().equals("")) {
                    query += getDBFieldName(entityField.getName()) + "='" + getter.invoke(entity) + "' AND ";
                }
            } catch (NoSuchMethodException e) {
                log.error("Problem in database querying...");
                log.error(e.getMessage());
                throw new DatabaseOperationException();
            } catch (InvocationTargetException e) {
                log.error("Problem in database querying...");
                log.error(e.getMessage());
                throw new DatabaseOperationException();
            } catch (IllegalAccessException e) {
                log.error("Problem in database querying...");
                log.error(e.getMessage());
                throw new DatabaseOperationException();
            }
        }
        query = query.substring(0, query.length() - 4);

        try {
            runQuery(query, DbOperation.DELETE);
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        }
    }


    public static <T extends BaseEntity> void create(T entity) throws DatabaseOperationException {
        Set<Field> entityFields = findFields(entity.getClass(), EntityField.class);

        String query = "INSERT INTO " + entity.getClass().getSimpleName() + " ";
        query += "(";
        for (Field entityField : entityFields) {
            query += getDBFieldName(entityField.getName()) + ", ";
        }
        query = query.substring(0, query.length() - 2);
        query +=")";

        query += "VALUES( ";


        for (Field entityField : entityFields) {

            Method getter = null;
            try {
                getter = entity.getClass().getMethod(getGetterMethodName(entityField.getName()));
                Object value = getter.invoke(entity);
                if(value == null) {
                    value = "";
                }
                query +=  "'" + value + "', ";
            } catch (NoSuchMethodException e) {
                log.error("Problem in database querying...");
                log.error(e.getMessage());
                throw new DatabaseOperationException();
            } catch (InvocationTargetException e) {
                log.error("Problem in database querying...");
                log.error(e.getMessage());
                throw new DatabaseOperationException();
            } catch (IllegalAccessException e) {
                log.error("Problem in database querying...");
                log.error(e.getMessage());
                throw new DatabaseOperationException();
            }
        }
        query = query.substring(0, query.length() - 2);
        query +=")";
        
        try {
            runQuery(query, DbOperation.INSERT);
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new DatabaseOperationException();
        }
    }


    public static List<Map<String, Object>> runQuery(String query, DbOperation operation) throws SQLException {
        QueryRunner run = new QueryRunner();

        ResultSetHandler<List<Map<String, Object>>> h = new ResultSetHandler<List<Map<String, Object>>>() {
            public List<Map<String, Object>> handle(ResultSet rs) throws SQLException {

                List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<String, Object>();
                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();

                    for (int i = 0; i < columnCount; i++) {
                        row.put(meta.getColumnName(i + 1), rs.getObject(i + 1));
                    }

                    rows.add(row);
                }

                return rows;
            }
        };


        Connection connection = Database.getConnection();
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        if(connection != null) {
            switch (operation) {
                case DELETE:
                    run.update(connection, query);
                    break;
                case INSERT:
                    result = run.insert(connection, query, h);
                    break;
                case SELECT:
                    result = run.query(connection, query, h);
                    break;
                case UPDATE:
                    run.update(connection, query);
                    break;
            }

        }
        return result;
    }


    public static <T extends BaseEntity> int getValidPrimaryKey(T entity) throws DeveloperFaultException, DatabaseOperationException {
        int primaryKey = 0;
        Set<Field> primaryKeyFields = findFields(entity.getClass(), PK.class);
        if(primaryKeyFields != null && !primaryKeyFields.isEmpty() && primaryKeyFields.size() ==1) {
            for (Field primaryKeyField : primaryKeyFields) {
                String query = "SELECT MAX(" + getDBFieldName(primaryKeyField.getName()) + ") FROM " + entity.getClass().getSimpleName();
                QueryRunner run = new QueryRunner();
                Connection connection = Database.getConnection();
                if(connection != null) {
                    ResultSetHandler<Integer> h = new ResultSetHandler<Integer>() {
                        public Integer handle(ResultSet rs) throws SQLException {

                            Integer nextPrimaryKey = 0;
                            while (rs.next()) {
                                nextPrimaryKey = (Integer)rs.getObject(1);
                            }
                            System.out.println("nextPrimaryKey______________"+nextPrimaryKey);
                            return nextPrimaryKey + 1;
                        }
                    };

                    try {
                        primaryKey = run.query(connection, query, h);
                        System.out.println("primaryKey______________"+primaryKey);
                    } catch (SQLException e) {
                        log.error(e.getMessage());
                        throw new DatabaseOperationException();
                    }
                }
            }
        } else {
            log.error("Primary key request error for : " + entity.getClass());
            throw new DeveloperFaultException();
        }
        return primaryKey;
    }

    private static String getGetterMethodName(String fieldName) {
        return "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1, fieldName.length());
    }

    private static String getSetterMethodName(String fieldName) {
        return "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1, fieldName.length());
    }

    private static String getEntityFieldName(String dbFieldName) {
        String entityFieldName = "";
        String[] fieldParts = dbFieldName.split("_");
        int i = 0;
        for (String fieldPart : fieldParts) {
            if(i == 0) {
                entityFieldName += fieldPart.toLowerCase();
            } else {
                String tempPart = fieldPart.toLowerCase();
                entityFieldName += tempPart.substring(0, 1).toUpperCase() + tempPart.substring(1, tempPart.length());
            }
            i++;
        }
        return entityFieldName;
    }

    private static String getDBFieldName(String entityFieldName) {
        String dbFieldName = "";
        if(entityFieldName.equals(entityFieldName.toLowerCase())) {
            dbFieldName = entityFieldName;
        } else {
            String[] fieldParts = entityFieldName.split("(?=\\p{Upper})");
            for (String fieldPart : fieldParts) {
                dbFieldName += fieldPart + "_";
            }
            dbFieldName = dbFieldName.substring(0, dbFieldName.length() - 1);
        }

        return dbFieldName;
    }

    private enum DbOperation {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
        GENERAL
    }
}