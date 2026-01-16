package ch.volleymanager.domain;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class EventTest {
    @Test
    void toStringVerifier() throws Exception{
        Event e = new Event();
        e.setEventId(1L);
        e.setEventDate(LocalDate.of(2022,06,01));
        e.setEventName("Auswärtsspiel Lunki");
        e.setEventLocation("Turnhalle Dorf, 8918 Oberlunkhofen");

        e.addTeamMember(
                new TeamMember(
                null,
                "Karin",
                "Tommer",
                "Bachmannweg",
                "7",
                8046,
                "Zürich",
                "karinsemail@gmail.com",
                "+4177 777 70 11",
                LocalDate.of(1999, 8, 6),
                "f",
                null,
                Collections.emptySet(),
                true,
                false
        )
    );
        e.addTeam(
                new Team (
                        null,
                        20,
                        "U20 Juniorinnen",
                        Collections.emptySet(),
                        Collections.emptySet()

        )
        );

        String temp = "Event{id=2, LocalDate=2022-06-01, Auswärtsspiel Lunki, 8918 Oberlunkhofen}";
        assertThat(temp).isEqualTo(e.toString());

    }
}