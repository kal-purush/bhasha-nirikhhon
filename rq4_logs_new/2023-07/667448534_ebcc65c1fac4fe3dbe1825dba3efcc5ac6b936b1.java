
import com.sg.supersightings.dto.Sighting;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import org.springframework.jdbc.core.RowMapper;


public class SightingsMapper implements RowMapper<Sighting>{

    @Override
    public Sighting mapRow(ResultSet rs, int rowNum) throws SQLException {
        Sighting sighting= new Sighting();
        sighting.setSightingID(rs.getInt("sightingID"));
        sighting.setDateSighted(rs.getObject("dateSighted", LocalDateTime.class));

        return sighting;
    }
    
}