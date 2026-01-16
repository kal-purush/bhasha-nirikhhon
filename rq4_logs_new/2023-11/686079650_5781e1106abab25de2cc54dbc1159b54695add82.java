package by.bsuir.poit.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Entity
@Table(name = "auction", schema = "auction_db")
public class Auction {
@Id
@GeneratedValue(strategy = GenerationType.IDENTITY)
@Column(name = "auction_id", nullable = false)
private Long id;

@NotNull
@Column(name = "start_date", nullable = false)
private Instant startDate;

@NotNull
@Column(name = "last_register_date", nullable = false)
private Instant lastRegisterDate;

@NotNull
@Column(name = "price_step", nullable = false, precision = 10, scale = 2)
private BigDecimal priceStep;

@Column(name = "members_limit")
private Integer membersLimit;

@Column(name = "end_date")
private Instant endDate;
@Builder.Default
@OneToMany(mappedBy = "auction", fetch = FetchType.LAZY)
private List<Lot> lots = new ArrayList<>();
@Builder.Default
@OneToMany(mappedBy = "auction", fetch = FetchType.LAZY)
private List<AuctionBet> bets = new ArrayList<>();
@NotNull
@ManyToOne(fetch = FetchType.LAZY, optional = false)
@JoinColumn(name = "auction_type_id", nullable = false)
private AuctionType auctionType;

@NotNull
@ManyToOne(fetch = FetchType.LAZY, optional = false)
@JoinColumn(name = "admin_user_id", nullable = false)
private User admin;
}