package br.com.klimber.inova.model;

import java.sql.Timestamp;
import java.time.Instant;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import lombok.Data;

@Entity
@Data
public class AccessLog {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;
	@NotNull
	private Long customerId;
	@NotBlank
	private String groupId;
	@NotBlank
	private String reportId;
	@NotNull
	private final Timestamp dataAcesso;

	public AccessLog(Long customerId, String groupId, String reportId) {
		super();
		this.customerId = customerId;
		this.groupId = groupId;
		this.reportId = reportId;
		this.dataAcesso = Timestamp.from(Instant.now());
	}

}