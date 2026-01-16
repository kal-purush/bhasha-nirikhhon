
package edu.ulatina.objects;


public class CustomersTO {
   private int cedula;
   private String email;
   private String name;
   private String lastname;
   private int state;

    public CustomersTO() {
    }

    public CustomersTO(int cedula, String email, String name, String lastname, int state) {
        this.cedula = cedula;
        this.email = email;
        this.name = name;
        this.lastname = lastname;
        this.state = state;
    }

    public int getCedula() {
        return cedula;
    }

    public void setCedula(int cedula) {
        this.cedula = cedula;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }
   
}

