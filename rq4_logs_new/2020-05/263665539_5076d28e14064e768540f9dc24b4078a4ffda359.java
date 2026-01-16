package sysjm3.bulbo.Bulbo.model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.GenericGenerator;
import org.springframework.format.annotation.DateTimeFormat;

@SuppressWarnings("serial")
public class User implements Serializable {
	

	public User() {
	}
	
	public User(String username) {
		this.username = username;
	}
	
	@Id
    @GeneratedValue(generator = "uuid2")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    @Column(name = "id", columnDefinition = "VARCHAR(255)", insertable = false, updatable = false, nullable = false)
    private UUID id;

    @Column(name = "username", nullable = false)
    private String username;
    
    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "email", nullable = false)
    private String email;

    @Column(name = "pass", nullable = false)
    private String password;

    @Temporal(TemporalType.DATE)
    @DateTimeFormat(pattern = "yyyy-MM-dd")
    @Column(name = "registerDate", nullable = false)
    private Date registerDate;
    
    /**
     * Getter for the field id
     *
     * @return UUID type value of the variable id
     */
    public UUID getId() {
        return id;
    }

    /**
     * Setter for the field id
     *
     * @param id UUID value to replace the current id value
     */
    public void setId(UUID id) {
        this.id = id;
    }
    
    /**
     * Getter for the field username
     *
     * @return String type value of the variable username
     */
    public String getUserName() {
        return username;
    }

    /**
     * Setter for the field username
     *
     * @param String username value to replace the current username value
     */
    public void setUserName(String username) {
        this.username = username;
    }
    
    /**
     * Getter for the field name
     *
     * @return String type value of the variable name
     */
    public String getName() {
        return name;
    }

    /**
     * Setter for the field name
     *
     * @param String name value to replace the current name value
     */
    public void setName(String name) {
        this.name = name;
    }
    
    /**
     * Getter for the field email
     *
     * @return String type value of the variable email
     */
    public String getEmail() {
        return email;
    }

    /**
     * Setter for the field email
     *
     * @param String value to replace the current email value
     */
    public void setEmail(String email) {
        this.email = email;
    }

    /**
     * TODO: Encrypt password
     * Getter for the field password
     *
     * @return String type value of the variable password
     */
    public String getPassword() {
        return password;
    }

    /**
     * TODO: Encrypt password
     * Setter for the field password
     *
     * @param String value to replace the current password value
     */
    public void setPassword(String password) {
        this.password = password;
    }
    
    /**
     * Getter for the field registerDate
     *
     * @return String type value of the variable registerDate
     */
    public String getRegisterDate() {
    	SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy");
    	return formatter.format(registerDate);
    }

    /**
     * Setter for the field name
     *
     * @param String value to replace the current registerDate value
     */
    public void setRegisterDate() {
    	Date date = new Date();
        this.registerDate = date;
    }

    @Override
    public String toString() {
        return "User{" + "id=" + id + ", username=" + username + ", name=" + name + ", email=" + email + ", password=" + password + ", registerDate=" + registerDate + "}";
    }
}