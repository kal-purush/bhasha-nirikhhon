package daos.meetings;

import entities.Meeting;
import util.ConnectUtil;

import java.sql.*;
import java.util.ArrayList;

public class MeetingDaoPostgres implements MeetingDao {

    @Override
    public ArrayList<Meeting> getAllMeetings() {
        try (Connection conn = ConnectUtil.getConnection()){
            String sql = "select * from meetings where meeting_id > 0";

            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();

            ArrayList<Meeting> meetingList = new ArrayList<Meeting>();

            while (rs.next()) {
                Meeting meeting = new Meeting();
                meeting.setMeetingId(rs.getInt("meeting_id"));
                meeting.setMeetingDate("meeting_date");
                meeting.setTopic("topic");
                meetingList.add(meeting);
            }

            return meetingList;
        }catch (SQLException e){
            e.printStackTrace();
        }

        return null;
    }
}