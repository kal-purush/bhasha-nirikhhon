package com.redhat.data.analytics.model;

import org.hibernate.annotations.GenericGenerator;
import org.joda.time.DateTime;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.util.Date;

/**
 * DataStream  store
 *
 * @author Zak Hassan <zak.hassan1010@redhat.com>
 */
@Entity
public class DataStream {

    public DataStream(String dataStreamId, String sourceId, String sinkId) {
        this.dataStreamId = dataStreamId;
        this.sourceId = sourceId;
        this.sinkId = sinkId;
    }

    @Id
    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    String dataStreamId;
    //from
    @Column(name="sourceId")
    String sourceId;

    // to
    @Column(name="sinkId")
    String sinkId;

    public String getDataStreamId() {
        return dataStreamId;
    }

    public void setDataStreamId(String dataStreamId) {
        this.dataStreamId = dataStreamId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSinkId() {
        return sinkId;
    }

    public void setSinkId(String sinkId) {
        this.sinkId = sinkId;
    }

    @Override
    public String toString() {
        return "DataStream{" +
                "dataStreamId='" + dataStreamId + '\'' +
                ", sourceId='" + sourceId + '\'' +
                ", sinkId='" + sinkId + '\'' +
                '}';
    }

    //TODO need to add these properties later.
//    DateTime createdDate;
//    DateTime expireDate;
//    int interval;
//    int startTime;
//    int delay;
//    DateTime createdDate;


}