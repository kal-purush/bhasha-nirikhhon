package ru.factsearch;

import com.datastax.driver.core.*;
import org.apache.commons.cli.*;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.util.stream.Collectors;

/**
 *
 */
public class Query {
    private static final int _batchSize = 1000;

    private static String[] keyColumnNames = new String[10];
    private static DataType[] keyColumnTypes;

    @SuppressWarnings("static-access")
    public static void main(String[] args){
        // Parse command line, set config variables
        Options options = new Options();
        options.addOption(OptionBuilder
                .withArgName("host address")
                .hasArg()
                .withDescription(" connection point node host")
                .withLongOpt("host")
                .create());
        options.addOption(OptionBuilder
                .withArgName("port")
                .hasArg()
                .withDescription(" connection point node port")
                .withLongOpt("port")
                .create());
        options.addOption(OptionBuilder
                .withArgName("username")
                .hasArg()
                .withDescription(" username for authentication")
                .withLongOpt("user")
                .create());
        options.addOption(OptionBuilder
                .withArgName("password")
                .hasArg()
                .withDescription(" password for authentication")
                .withLongOpt("pass")
                .create());
        options.addOption(OptionBuilder
                .withArgName("cql")
                .hasArg()
                .withDescription(" CQL query")
                .withLongOpt("cql")
                .isRequired()
                .create());
        options.addOption(OptionBuilder
                .withArgName("column_name, column_name")
                .hasArgs(10)
                .withValueSeparator(',')
                .isRequired()
                .withDescription("Names of key columns. If there are more than one bigint/int/varint column specified Sphinx key will be generated from key columns using hash function.")
                .withLongOpt("keys")
                .create());
        CommandLineParser parser = new BasicParser();
        String cql = "";
        String connectionPointHost = "";
        int connectionPointPort = 0;
        String user = null;
        String pass = null;
        try {
            CommandLine cmd = parser.parse( options, args );
            if (cmd.hasOption("host")) {
                connectionPointHost = cmd.getOptionValue("host");
            } else {
                connectionPointHost = "localhost";
            }
            if (cmd.hasOption("port")) {
                connectionPointPort = Integer.parseInt(cmd.getOptionValue("port"));
            } else {
                connectionPointPort = 9042;
            }
            if (cmd.hasOption("user")){
                user = cmd.getOptionValue("user");
                if (cmd.hasOption("pass")) {
                    pass = cmd.getOptionValue("pass");
                } else {
                    pass = "";
                }
            }
            keyColumnNames = cmd.getOptionValues("keys");
            cql = cmd.getOptionValue("cql");
        } catch (ParseException|NumberFormatException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("query -keys col1,col2,... [-other_options] -cql <CQL QUERY>\n", options);
            System.exit(-1);
        }

        Cluster cluster;
        if (user != null) {
            cluster = Cluster.builder()
                    .addContactPoint(connectionPointHost)
                    .withPort(connectionPointPort)
                    .withCredentials(user, pass)
                    .build();
        } else {
            cluster = Cluster.builder()
                    .addContactPoint(connectionPointHost)
                    .withPort(connectionPointPort)
                    .build();
        }
        Session session = cluster.connect();
        XMLOutputFactory xof =  XMLOutputFactory.newInstance();
        try {
            XMLStreamWriter writer = xof.createXMLStreamWriter(System.out);
            writer.writeStartDocument("utf-8", "1.0");
            writer.setPrefix("sphinx", "sphinx");
            writer.writeStartElement("sphinx","docset");
            Statement statement = new SimpleStatement(cql);
            statement.setFetchSize(_batchSize);

            ResultSet rs = session.execute(statement);
            while (!rs.isExhausted()) {
                for (Row row: rs){
                    processRow(row, writer, rs.getColumnDefinitions());
                }
            }

            writer.writeEndElement(); // sphinx:docset
            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        session.close();
        cluster.close();
    }

    private static void processRow(Row row, XMLStreamWriter writer, ColumnDefinitions columnDefinitions) throws XMLStreamException{
        writer.writeStartElement("sphinx", "document");
        writer.writeAttribute("id", getId(row, columnDefinitions));
        for (ColumnDefinitions.Definition definition: columnDefinitions) {
            writer.writeStartElement(definition.getName());
            writer.writeCharacters(getValue(row, definition.getName(), definition.getType()));
            writer.writeEndElement();
        }
        writer.writeEndElement(); // sphinx:document
    }

    private static String getId(Row row, ColumnDefinitions columnDefinitions){
        if (keyColumnTypes == null) {
            keyColumnTypes = new DataType[keyColumnNames.length];
            for(int i = 0; i < keyColumnNames.length; i++){
                keyColumnTypes[i] = columnDefinitions.getType(keyColumnNames[i]);
            }
        }
        if (keyColumnNames.length == 1 &&
                (DataType.bigint().equals(keyColumnTypes[0]) || DataType.cint().equals(keyColumnTypes[0]) || DataType.varint().equals(keyColumnTypes[0]))) {
            return getValue(row, keyColumnNames[0], keyColumnTypes[0]);
        }

        long hashBase = 0;
        String str = "";
        for (int i = 0; i < keyColumnNames.length; i++){
            // If at least one of key columns is int or long use it for hashBase
            if ((DataType.cint().equals(keyColumnTypes[i]) || DataType.bigint().equals(keyColumnTypes[i])) && hashBase == 0) {
                hashBase = row.getInt(keyColumnNames[i]);
            } else {
                // other columns concatenate into big string
                str += getValue(row, keyColumnNames[i], keyColumnTypes[i]) + " ";
            }
        }

        return Long.toString(getStringKey(hashBase, str));
    }

    private static String getValue(Row row, String name, DataType dataType){
        if (DataType.cint().equals(dataType)){
            return Integer.toString(row.getInt(name));
        } else if (DataType.bigint().equals(dataType)){
            return Long.toString(row.getLong(name));
        } else if (DataType.ascii().equals(dataType) || DataType.text().equals(dataType) || DataType.varchar().equals(dataType)){
            return row.getString(name);
        } else if (DataType.cboolean().equals(dataType)){
            return Boolean.toString(row.getBool(name));
        } else if (DataType.cdouble().equals(dataType)){
            return Double.toString(row.getDouble(name));
        } else if (DataType.blob().equals(dataType)){
            return "<![CDATA[" + row.getBytes(name).toString() + "]]>";
        } else if (DataType.cfloat().equals(dataType)){
            return Float.toString(row.getFloat(name));
        } else if (DataType.counter().equals(dataType)){
            return Integer.toString(row.getInt(name));
        } else if (DataType.decimal().equals(dataType)){
            return row.getDecimal(name).toString();
        } else if (DataType.inet().equals(dataType)){
            return row.getInet(name).toString();
        } else if (DataType.timestamp().equals(dataType) || DataType.timeuuid().equals(dataType)){
            return row.getDate(name).toString();
        } else if (DataType.varint().equals(dataType)) {
            return row.getVarint(name).toString();
        } else {
            for (DataType primitiveType: DataType.allPrimitiveTypes()){
                if (DataType.set(primitiveType).equals(dataType)) {
                    return row.getSet(name, primitiveType.asJavaClass()).stream()
                            .map(Object::toString)
                            .collect(Collectors.joining(" "));
                } else if (DataType.list(primitiveType).equals(dataType)) {
                    return row.getList(name, primitiveType.asJavaClass()).stream()
                            .map(Object::toString)
                            .collect(Collectors.joining(" "));
                }
            }
        }

        return "";
    }

    public static long getStringKey (long hashBase, String str){
        if (str == null) {
            return 0;
        }
        long hash = hashBase;
        for (char c:str.toCharArray()) {
            hash = c + (hash << 6) + (hash << 16) - hash;
        }
        if (hash > 0) {
            return hash;
        } else {
            return ~hash + 1;
        }
    }
}