package com.happiday.Happi_Day.initialization.service;

import com.happiday.Happi_Day.domain.entity.artist.Artist;
import com.happiday.Happi_Day.domain.entity.artist.ArtistSubscription;
import com.happiday.Happi_Day.domain.entity.team.Team;
import com.happiday.Happi_Day.domain.entity.team.TeamSubscription;
import com.happiday.Happi_Day.domain.entity.user.User;
import com.happiday.Happi_Day.domain.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class SubscriptionInitService {

    private final UserRepository userRepository;
    private final ArtistRepository artistRepository;
    private final TeamRepository teamRepository;
    private final ArtistSubscriptionRepository artistSubscriptionRepository;
    private final TeamSubscriptionRepository teamSubscriptionRepository;

    @Transactional
    public void initSubscription() {
        User user = userRepository.findById(2L).orElse(null);

        // 아티스트 구독 추가
        Artist artist1 = artistRepository.findById(1L).orElse(null);
        Artist artist2 = artistRepository.findById(2L).orElse(null);
        if (artist1 != null && artist2 != null) {
            addArtistSubscription(user, artist1);
            addArtistSubscription(user, artist2);
        }

        // 팀 구독 추가
        Team team1 = teamRepository.findById(1L).orElse(null);
        Team team2 = teamRepository.findById(2L).orElse(null);
        if (team1 != null && team2 != null) {
            addTeamSubscription(user, team1);
            addTeamSubscription(user, team2);
        }
    }

    private void addArtistSubscription(User user, Artist artist) {
        boolean isAlreadySubscribed = artistSubscriptionRepository.existsByUserAndArtist(user, artist);
        if (!isAlreadySubscribed) {
            ArtistSubscription subscription = ArtistSubscription.builder()
                    .user(user)
                    .artist(artist)
                    .build();
            artistSubscriptionRepository.save(subscription);
        }
    }

    private void addTeamSubscription(User user, Team team) {
        boolean isAlreadySubscribed = teamSubscriptionRepository.existsByUserAndTeam(user, team);
        if (!isAlreadySubscribed) {
            TeamSubscription subscription = TeamSubscription.builder()
                    .user(user)
                    .team(team)
                    .build();
            teamSubscriptionRepository.save(subscription);
        }
    }
}