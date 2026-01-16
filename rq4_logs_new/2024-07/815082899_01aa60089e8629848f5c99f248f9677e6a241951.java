package db.wrapper;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface BaseWrapper<T> {
    /**
     * 执行SQL语句
     * @return
     */
    public T execute() throws SQLException;

    /**
     * 生成语句
     * @return
     */
    public PreparedStatement generateSQL() throws ParserConfigurationException, IOException, ClassNotFoundException, InvocationTargetException, SAXException, InstantiationException, IllegalAccessException, NoSuchMethodException, SQLException;

}