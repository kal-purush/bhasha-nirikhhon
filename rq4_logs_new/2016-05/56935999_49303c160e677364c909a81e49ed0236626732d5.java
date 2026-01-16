package ucsp.is2.model;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;

@Entity
public class Song {

	@Id
	@SequenceGenerator(name="SONG_ID_GENERATOR", sequenceName="SONG_ID_SEQ")
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="SONG_ID_GENERATOR")
	private Long id;

	private String name;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}