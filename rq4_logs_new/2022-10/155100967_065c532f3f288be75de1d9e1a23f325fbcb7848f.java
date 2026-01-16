package crtracker.plugins.riverrace;

import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import crtracker.persistency.dao.MeasureDao;
import crtracker.persistency.model.CrTrackerTypes;
import crtracker.plugin.AbstractPlugin;
import crtracker.plugin.PluginEvent;
import jcrapi2.api.intern.clans.riverracelog.Clan;
import jcrapi2.api.intern.clans.riverracelog.Participant;
import jcrapi2.api.intern.clans.riverracelog.RiverRaceLog;
import jcrapi2.api.intern.clans.riverracelog.RiverRaceLogResponse;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class RiverRaceLogPlugin extends AbstractPlugin<RiverRaceLogPluginEvent> {

  @Autowired
  private MeasureDao measureDao;

  @Override
  public void onPluginEvent(Session session, RiverRaceLogPluginEvent riverRaceLogPluginEvent) {
    RiverRaceLogResponse
        riverRaceLogResponse =
        riverRaceLogPluginEvent.getRiverRaceLogResponse();
    Map<String, Integer> scores = new HashMap<>();
    for (RiverRaceLog riverRaceLog : riverRaceLogResponse.getItems()) {
      Clan clan = getClan(riverRaceLog);
      for (Participant participant : clan.getParticipants()) {
        String playerTag = participant.getTag();
        Integer fame = scores.get(playerTag);
        if (fame == null) {
          fame = 0;
        }
        fame += participant.getFame();
        scores.put(playerTag, fame);
      }
    }
    for (Map.Entry<String, Integer> entry : scores.entrySet()) {
      String playerTag = entry.getKey();
      int fame = entry.getValue();
      measureDao.updateNumberMeasure(session, playerTag, CrTrackerTypes.MEMBER_LAST_10_RIVER_WARS_FAME.getCode(), fame);
    }
  }

  private Clan getClan(RiverRaceLog riverRaceLog) {
    String clanTag = configurationService.getClanTag();
    return riverRaceLog.getStandings().stream().filter(standing -> clanTag.equals(standing.getClan().getTag()))
        .findFirst().get().getClan();
  }

  @Override
  public boolean canHandlePluginEvent(PluginEvent pluginEvent) {
    return pluginEvent instanceof RiverRaceLogPluginEvent;
  }

}