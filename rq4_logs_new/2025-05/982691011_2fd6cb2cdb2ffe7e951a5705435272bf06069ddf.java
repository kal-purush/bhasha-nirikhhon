import java.time.LocalDateTime;



import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "Results")
public class Results {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "result_id")
    private int resultId;

    @ManyToOne
    @JoinColumn(name = "candidate_id", referencedColumnName = "candidate_id", insertable = false, updatable = false)
    private Candidates candidate;

    @ManyToOne
    @JoinColumn(name = "election_id", referencedColumnName = "election_id", insertable = false, updatable = false)
    private Elections election;

    @Column(name = "total_votes", nullable = false)
    private int totalVotes;

    @Column(name = "declared_at")
    private LocalDateTime declaredAt;
    
    public Results() {
    	super();
    }

	public Results(int resultId, Candidates candidate, Elections election, int totalVotes, LocalDateTime declaredAt) {
		super();
		this.resultId = resultId;
		this.candidate = candidate;
		this.election = election;
		this.totalVotes = totalVotes;
		this.declaredAt = declaredAt;
	}

	public int getResultId() {
		return resultId;
	}

	public void setResultId(int resultId) {
		this.resultId = resultId;
	}

	public Candidates getCandidate() {
		return candidate;
	}

	public void setCandidate(Candidates candidate) {
		this.candidate = candidate;
	}

	public Elections getElection() {
		return election;
	}

	public void setElection(Elections election) {
		this.election = election;
	}

	public int getTotalVotes() {
		return totalVotes;
	}

	public void setTotalVotes(int totalVotes) {
		this.totalVotes = totalVotes;
	}

	public LocalDateTime getDeclaredAt() {
		return declaredAt;
	}

	public void setDeclaredAt(LocalDateTime declaredAt) {
		this.declaredAt = declaredAt;
	}

	@Override
	public String toString() {
		return "Results [resultId=" + resultId + ", totalVotes=" + totalVotes + ", declaredAt=" + declaredAt + "]";
	}
    
}


    
    
    
    
    