package e_appliance_warehouse.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Entity
@Table(name = "brand")
@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
@AllArgsConstructor
public class Brand extends CommonColumns {

	private static final long serialVersionUID = 1L;

	@Id
	@SequenceGenerator(name = "brandseq", sequenceName = "brand_seq", initialValue = 401, allocationSize = 1)
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "brandseq")
	@Column(name = "brand_id")
	private Integer brandId;

	@Column(name = "brand_name", nullable = false, unique = true)
	private String brandName;
	
}