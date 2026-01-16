package com.example.dietetyk.patient.physicalctivity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.example.dietetyk.patient.Patient;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "physical_activities")
public class PhysicalActivity implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "physical_activity_id")
	private Long id;
	@Column(name = "degree_of_physical_activity")
	private String degreeOfPhysicalActivity;
	@OneToMany(mappedBy = "physicalActivity", 
			cascade = { CascadeType.REMOVE })
	private List<Patient> patients = new ArrayList<>();
}