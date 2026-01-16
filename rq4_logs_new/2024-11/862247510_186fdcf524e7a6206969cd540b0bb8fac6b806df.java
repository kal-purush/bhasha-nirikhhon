package com.example.FenceLink.MatchRank;
import com.example.FenceLink.tournament.Tournament;
import com.fasterxml.jackson.annotation.JsonIgnore;

import jakarta.persistence.*;
import lombok.*;

@Data
@Entity
@Table(name = "match_ranks")
@NoArgsConstructor
public class MatchRank {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(cascade = {CascadeType.PERSIST, CascadeType.MERGE})
    @JoinColumn(name = "tournament_id", referencedColumnName = "id", nullable = false)
    @JsonIgnore
    private Tournament tournament;

    @Column(name = "player_id", nullable = false)
    private Long playerId;

    @Column(name = "win_count", nullable = false)
    private int winCount = 0;

    @Column(name = "loss_count", nullable = false)
    private int lossCount = 0;

    @Column(name = "current_rank", nullable = false)
    private int currentRank = 0;

    @Column(name = "is_eliminated", nullable = false)
    private boolean isEliminated = false;

    public MatchRank(Long id, Tournament tournament, Long playerId, int winCount, int lossCount, int currentRank, boolean isEliminated) {
        this.id = id;
        this.tournament = tournament;
        this.playerId = playerId;
        this.winCount = winCount;
        this.lossCount = lossCount;
        this.currentRank = currentRank;
        this.isEliminated = isEliminated;
    }
    


}

