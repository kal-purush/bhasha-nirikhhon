package hibernateExample;

/*
 *This java class sets up our object that our application can manipulate and be used
 *to update the database. In this case, our "Employee" has 5 characteristics. 
 *An id, a first name, a last name, a middle initial, and a position in the company.
 */
public class Employee {  
private int id;  
private String firstName,lastName, middleInitial, position;  
  
public int getId() {  
    return id;  
}  
public void setId(int id) {  
    this.id = id;  
}  
public String getFirstName() {  
    return firstName;  
}  
public void setFirstName(String firstName) {  
    this.firstName = firstName;  
}  
public String getLastName() {  
    return lastName;  
}  
public void setLastName(String lastName) {  
    this.lastName = lastName;  
}  
public String getMiddleInitial() {  
    return middleInitial;  
}  
public void setMiddleInitial(String middleInitial) {  
    this.middleInitial = middleInitial;  
}  

public String getPosition() {  
    return position;  
}  
public void setPosition(String position) {  
    this.position = position;  
}
  
  
  
}  