package TT.tripTogether.domain;

@Entity
@Getter
@NoArgsConstructor
@Table(name ="Application")
public class Application extends BaseEntity{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "Application_id")
    @Column(updatable = false)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    private User user;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "post_id")
    private Post post;

    @Enumerated(EnumType.STRING)
    private Content content;

    public Application(Long id, User user, Posting posting, Content content) {
        this.id = id;
        this.user = user;
        this.posting = posting;
        this.content = content;
    }
}