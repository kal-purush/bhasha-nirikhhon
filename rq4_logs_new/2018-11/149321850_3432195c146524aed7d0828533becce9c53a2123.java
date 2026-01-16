package hu.uni.miskolc.iit.impl;

import hu.uni.miskolc.iit.dao.TeacherServiceDao;
import hu.uni.miskolc.iit.model.Form;
import hu.uni.miskolc.iit.persist.AbstractJdbc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class TeacherServiceDaoImpl extends AbstractJdbc implements TeacherServiceDao {

    @Autowired
    @Qualifier(value = "teacherServiceDao")
    private Properties sqlStatements;


    @Override
    public List<Form> getForms() {


        String sql = sqlStatements.getProperty("select.all.forms");
        return this.getJdbc().query(sql, new TeacherServiceDaoImpl.TeacherFormsMapper());
    }


    class TeacherFormsMapper implements RowMapper<Form> {

        @Override
        public Form mapRow(ResultSet resultSet, int rowNum) throws SQLException {
            Form form = new Form();
            form.setCourseID(resultSet.getString("courseId"));
            form.setStudentID(resultSet.getString("studentId"));
            form.setTeacherID(resultSet.getString("teacherId"));
            form.setText(resultSet.getString("text"));
            form.setDate(resultSet.getDate("date"));
            //form.setCurrentState(resultSet.getString("state"));

            return form;
        }
    }
}