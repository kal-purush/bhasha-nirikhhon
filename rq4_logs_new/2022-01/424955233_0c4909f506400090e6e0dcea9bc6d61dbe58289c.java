package ch.volleymanager.config;

import ch.volleymanager.domain.*;
import ch.volleymanager.repo.*;
import ch.volleymanager.utils.HasLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.LocalDate;
import java.util.List;

/**
 * Defines a Bean for the dev-Profile
 */
@Configuration
@Profile("dev")
@Transactional
// @ConditionalOnClass(name = {"org.h2.Driver"})
public class DevConfiguration implements HasLogger {

    private Long felixMusterId;
    private Long maxMusterMannId;
    private Long johnDoeId;

    private Long teamMax14AgedId;

    private Long teamMemberAnnaMuster14YearsId;
    private Long teamMemberJuliaMustermann14YearsId;
    private Long teamMemberJennyDoe14YearsId;

    private Long eventTurnierFebruarId;
    private Long eventTurnierMarchId;

    @Autowired
    AbstractPersonRepo abstractPersonRepo;

    @Autowired
    ContactPersonRepo contactPersonRepo;

    @Autowired
    EventRepo eventRepo;

    @Autowired
    TeamMemberRepo teamMemberRepo;

    @Autowired
    TeamRepo teamRepo;

    public DevConfiguration() {
        getLogger().info("Dev Configuration Class");
    }

    @PostConstruct
    public void createData() {
        createPersonData();
        createTeamData();
        createTeamMemberData();
        createEventData();
        assignContactPersonToTeamMember();
        assignTeamMemberToTeam();
        assignTeamToEvent();
        assignTeamMemberToEvent();
    }

    private void createPersonData() {
        ContactPerson cpFelixMuster = new ContactPerson(null, "Felix", "Muster", "Bahnhofstrasse", 1, 5000, "Aarau");
        ContactPerson cpMaxMusterMann = new ContactPerson(null, "Max", "Mustermann", "Alpenblick", 1, 3000, "Bern");
        ContactPerson cpJohnDoe = new ContactPerson(null, "John", "Doe", "Lagerstrasse", 41, 8004, "Zürich");

        cpFelixMuster = contactPersonRepo.save(cpFelixMuster);
        cpMaxMusterMann = contactPersonRepo.save(cpMaxMusterMann);
        cpJohnDoe = contactPersonRepo.save(cpJohnDoe);

        felixMusterId = cpFelixMuster.getId();
        maxMusterMannId = cpMaxMusterMann.getId();
        johnDoeId = cpJohnDoe.getId();

        getLogger().debug("Person felixMuster, maxMustermann and john doe are saved to DB");

        List<ContactPerson> persons = contactPersonRepo.findQueryByLastName("Mustermann");
        persons.forEach(person -> getLogger().debug("findQueryByLastName Mustermann = " + person.toString()));

        abstractPersonRepo.findAll(Sort.by(Sort.Direction.ASC, "lastName")).forEach(person -> {
            abstractPersonRepo.findById(person.getId())
                    .ifPresent(p -> getLogger().debug("findAll, Sort by lastName ASC = " + p.toString()));
        });
    }

    private void createTeamData() {

        Team teamMax14Aged = new Team (null, 14, "Max14Aged", null, null);
        teamRepo.save(teamMax14Aged);
        teamMax14AgedId = teamMax14Aged.getTeamId();
    }

    private void createTeamMemberData() {

        TeamMember teamMemberAnnaMuster14Years = new TeamMember(null, "Anna", "Muster", "Bahnhofstrasse", 1, 5000, "Aarau", LocalDate.of(2008, 3, 3), null, null, false);
        teamMemberAnnaMuster14Years = teamMemberRepo.save(teamMemberAnnaMuster14Years);
        teamMemberAnnaMuster14YearsId = teamMemberAnnaMuster14Years.getId();

        TeamMember teamMemberJuliaMustermann14Years = new TeamMember(null, "Julia", "Mustermann", "Alpenblick", 1, 3000, "Bern", LocalDate.of(2008, 4, 3), null, null, false);
        teamMemberJuliaMustermann14Years = teamMemberRepo.save(teamMemberJuliaMustermann14Years);
        teamMemberJuliaMustermann14YearsId = teamMemberJuliaMustermann14Years.getId();

        TeamMember teamMemberJennyDoe14Years = new TeamMember(null, "Anna", "Muster", "Lagerstrasse", 41, 8004, "Zürich", LocalDate.of(2008, 5, 3), null, null, false);
        teamMemberJennyDoe14Years = teamMemberRepo.save(teamMemberJennyDoe14Years);
        teamMemberJennyDoe14YearsId = teamMemberJennyDoe14Years.getId();
    }

    private void createEventData() {
        Event eventTournamentFebruary = new Event(null, "Februar Turnier", LocalDate.of(2022, 2, 12), "my Job", "Volley Ball Halle", 5, false, null, null);
        eventTournamentFebruary = eventRepo.save(eventTournamentFebruary);
        eventTurnierFebruarId = eventTournamentFebruary.getEventId();

        Event eventTournamentMarch = new Event(null, "März Turnier", LocalDate.of(2022, 3, 15), "new Job", "Volley Ball Halle", 5, false, null, null);
        eventTournamentMarch = eventRepo.save(eventTournamentMarch);
        eventTurnierMarchId = eventTournamentMarch.getEventId();
    }

    private void assignContactPersonToTeamMember() {

        assignContactPersonToTeamMember(felixMusterId, teamMemberAnnaMuster14YearsId);
        assignContactPersonToTeamMember(maxMusterMannId, teamMemberJuliaMustermann14YearsId);
        assignContactPersonToTeamMember(johnDoeId, teamMemberJennyDoe14YearsId);

    }

    private void assignContactPersonToTeamMember(Long contactId, Long teamMemberId) {

        contactPersonRepo.findByIdWithEagerRelationships(contactId).ifPresent(contactPerson -> {
            teamMemberRepo.findByIdWithEagerRelationships(teamMemberId).ifPresent(teamMember -> {
                teamMember.setContactPerson(contactPerson);
                contactPerson.addTeamMember(teamMember);
                teamMemberRepo.save(teamMember);
                contactPersonRepo.save(contactPerson);
            });
        });
    }

    private void assignTeamMemberToTeam() {
        assignTeamMemberToTeam(teamMemberAnnaMuster14YearsId, teamMax14AgedId);
        assignTeamMemberToTeam(teamMemberJuliaMustermann14YearsId, teamMax14AgedId);
        assignTeamMemberToTeam(teamMemberJennyDoe14YearsId, teamMax14AgedId);
    }

    private void assignTeamMemberToTeam(Long teamMemberId, Long teamId) {
        teamRepo.findByIdWithEagerRelationships(teamId).ifPresent(team -> {
            teamMemberRepo.findByIdWithEagerRelationships(teamMemberId).ifPresent(teamMember -> {
                teamMember.addTeam(team);
                team.addTeamMember(teamMember);
                teamMemberRepo.save(teamMember);
                teamRepo.save(team);
            });
        });
    }

    private void assignTeamToEvent() {
        assignTeamToEvent(teamMax14AgedId, eventTurnierFebruarId);
    }

    private void assignTeamToEvent(Long teamId, Long eventId) {
        teamRepo.findByIdWithEagerRelationships(teamId).ifPresent(team -> {
            eventRepo.findByIdWithEagerRelationships(eventId).ifPresent(event -> {
                event.addTeam(team);
                team.addEvent(event);
                eventRepo.save(event);
                teamRepo.save(team);
            });
        });
    }

    private void assignTeamMemberToEvent() {
        assignTeamMemberToEvent(teamMemberAnnaMuster14YearsId, eventTurnierMarchId);
    }

    private void assignTeamMemberToEvent(Long teamMemberId, Long eventId) {
        teamMemberRepo.findByIdWithEagerRelationships(teamMemberId).ifPresent(teamMember -> {
            eventRepo.findByIdWithEagerRelationships(eventId).ifPresent(event -> {
                event.addTeamMember(teamMember);
                teamMember.addEvent(event);
                eventRepo.save(event);
                teamMemberRepo.save(teamMember);
            });
        });
    }

}