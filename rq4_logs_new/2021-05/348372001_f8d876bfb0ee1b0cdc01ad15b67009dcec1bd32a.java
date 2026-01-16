package pl.zgora.uz.wiea.tna.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.Setter;
import pl.zgora.uz.wiea.tna.util.DateTimeDeserializer;
import pl.zgora.uz.wiea.tna.util.DateTimeSerializer;

import java.time.OffsetDateTime;

@Getter
@Setter
public class AttendanceRecordEntryTime {

    @JsonProperty
    @JsonSerialize(using = DateTimeSerializer.class)
    @JsonDeserialize(using = DateTimeDeserializer.class)
    private OffsetDateTime enteredAt;
}