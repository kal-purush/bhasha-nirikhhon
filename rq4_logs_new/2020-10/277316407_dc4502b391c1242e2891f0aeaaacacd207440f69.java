package ru.otus.jdbc.mapper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class EntitySQLMetaDataByClassMeta implements EntitySQLMetaData {

    private final EntityClassMetaData<?> metaData;

    public EntitySQLMetaDataByClassMeta(EntityClassMetaData<?> metaData) {
        this.metaData = metaData;
    }

    @Override
    public String getSelectAllSql() {
        return String.format("select * from %s", getTableName());
    }

    @Override
    public String getSelectByIdSql() {
        return String.format("select * from %s where %s = ?",
                getTableName(), getIdFieldName());
    }

    @Override
    public String getInsertSql() {
        Collection<String> namesForInsert = getNamesForInsert();
        String templateValues = createTemplateValues(namesForInsert);
        return String.format("insert into %s (%s) values (%s)",
                getTableName(), String.join(", ", namesForInsert), templateValues);
    }

    @Override
    public String getUpdateSql() {
        String updateMatches = createUpdateMatches(getNamesForUpdate());
        return String.format("update %s set %s where %s = ?",
                getTableName(), updateMatches, getIdFieldName());
    }

    private String getTableName() {
        return metaData.getName().toLowerCase();
    }

    private String getIdFieldName() {
        return metaData.getIdField().getName();
    }

    private Collection<String> getNamesForInsert() {
        return metaData.getFieldsWithoutId().stream()
                .map(Field::getName)
                .collect(Collectors.toList());
    }

    private Collection<String> getNamesForUpdate() {
        return getNamesForInsert();
    }

    private String createTemplateValues(Collection<String> names) {
        String templateValues = "?, ".repeat(names.size());
        return templateValues.substring(0, templateValues.length() - 2);
    }

    private String createUpdateMatches(Collection<String> names) {
        List<String> matches = new ArrayList<>(names.size());
        names.forEach(name -> matches.add(name + " = ?"));
        return String.join(", ", matches);
    }

}