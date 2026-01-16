/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleregister;
/**
 *
 * @author sean
 */
public class Address {
    private String street;
    private String city;
    
    public Address(String aStreet, String aCity){
        street = aStreet;
        city = aCity;
    }

    @Override
    public String toString() {
        return street + " " + city;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 23 * hash + (this.street != null ? this.street.hashCode() : 0);
        hash = 23 * hash + (this.city != null ? this.city.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Address other = (Address) obj;
        if ((this.street == null) ? (other.street != null) : !this.street.equals(other.street)) {
            return false;
        }
        if ((this.city == null) ? (other.city != null) : !this.city.equals(other.city)) {
            return false;
        }
        return true;
    }
    
    
}