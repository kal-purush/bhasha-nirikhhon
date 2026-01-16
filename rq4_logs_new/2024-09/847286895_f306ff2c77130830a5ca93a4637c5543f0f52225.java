package ir.freeland.springboot.persistence.model;


import jakarta.persistence.CascadeType;

/*import java.util.Date;*/

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
/*import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;*/
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
/*import jakarta.persistence.Temporal;
/*import jakarta.persistence.TemporalType;*/
//import jakarta.persistence.Transient;

@Entity
@Table(name="corruptedItem")
public class CorruptedItem {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name = "id")
	private Long id;
	
	@OneToOne(cascade = CascadeType.ALL) //There will be a unique constraint 
    @JoinColumn(name = "item_id", referencedColumnName = "id")
    private Item item;

	@Column(name = "reason", length = 100, nullable = true, unique = false)
	private String reason;
	

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}
	
	public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	

}