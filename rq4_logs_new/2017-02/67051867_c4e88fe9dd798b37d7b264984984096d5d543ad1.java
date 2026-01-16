package de.sjka.logstash.osgi;

import org.json.simple.JSONObject;
import org.osgi.service.log.LogEntry;

/**
 * Serializer for LogEntries.
 * Converts the entries to JSONObjects.
 * 
 * @author Christoph Knauf - Initial contribution and API.
 *
 */
public interface ILogstashSerializer {

    /**
     * Serializes {@link LogEntry} to {@link JSONObject}
     * 
     * @param logEntry the LogEntry to serialize
     * @return the serialized LogEntry
     */
    JSONObject serialize(LogEntry logEntry); 

}