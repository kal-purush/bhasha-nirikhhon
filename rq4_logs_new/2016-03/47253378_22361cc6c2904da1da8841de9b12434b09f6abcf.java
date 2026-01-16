package de.alpharogroup.payment.system.entities;

import javax.persistence.Entity;
import javax.persistence.Table;

import de.alpharogroup.db.entity.VersionableBaseEntity;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


/**
 * The class {@link Paymentmethods} holds the name of the payment method.
 */
@Entity
@Table(name="paymentmethods")
@Getter
@Setter
@NoArgsConstructor
public class Paymentmethods
extends VersionableBaseEntity<Integer>
implements Cloneable {

	/** Serial Version UID */
	private static final long serialVersionUID = 1L;

	/** The name. */
	private String name;

}