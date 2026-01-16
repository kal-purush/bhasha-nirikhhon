package org.citybike;

import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.jpa.domain.AbstractPersistable;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
public class Journey extends AbstractPersistable<Long> {

    private Timestamp departureTimeStamp;
    private Timestamp returnTimestamp;
    @OneToOne
    @JoinColumn(name = "departure_station_id")
    private Station departureStation;
    @OneToOne
    @JoinColumn(name = "return_station_id")
    private Station returnStation;
    private double distance;
    private double duration;

}