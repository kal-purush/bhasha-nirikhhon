package nl.sean.dea.persistence;

import nl.sean.dea.dto.TrackDTO;
import nl.sean.dea.dto.TracksDTO;

import javax.enterprise.inject.Default;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Default
public class TrackDAOImpl implements TrackDAO {
    @Override
    public TracksDTO getAllTracks() {
        List<TrackDTO> foundTracks;
        try (
                Connection connection = new ConnectionFactory().getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(
                        "SELECT * FROM track")
        ) {
            ResultSet resultSet = preparedStatement.executeQuery();
            foundTracks = getTracksFromResultset(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException();
        }
        return new TracksDTO(foundTracks);
    }

    @Override
    public TracksDTO getAllTracksFromPlaylist(int playlistID) {
        List<TrackDTO> foundTracks;
        try (
                Connection connection = new ConnectionFactory().getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(
                        "SELECT * FROM track JOIN track_in_playlist ON track.track_ID = track_in_playlist.track_ID WHERE track_in_playlist.playlist_ID=?")
        ) {
            preparedStatement.setInt(1, playlistID);
            ResultSet resultSet = preparedStatement.executeQuery();
            foundTracks = getTracksFromResultset(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException();
        }
        return new TracksDTO(foundTracks);
    }

    private List<TrackDTO> getTracksFromResultset(ResultSet resultSet) throws SQLException {
        List<TrackDTO> foundTracks = new ArrayList<>();

        while (resultSet.next()) {
            foundTracks.add(new TrackDTO(
                    resultSet.getInt("track_ID"),
                    resultSet.getString("title"),
                    resultSet.getString("performer"),
                    resultSet.getInt("duration"),
                    resultSet.getString("album"),
                    resultSet.getInt("playcount"),
                    resultSet.getString("publicationDate"),
                    resultSet.getString("description"),
                    resultSet.getBoolean("offlineAvailable")));
        }

        return foundTracks;
    }
}