package br.com.pedrorafante.dslist.entities;

import jakarta.persistence.*;
import lombok.*;

@Table(name = "tb_belonging")
@Entity(name = "Belonging")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(of = "id")
public class Belonging {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "game_id")
    private Game game;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "gameList_id")
    private GameList gameList;
    private Integer position;
}