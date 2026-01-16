package core;

public class Employee
{
    private int empID;
    private Customer servicedCustomer;
    private Vehicle rentedCar;        

    /**
     * @return the empID
     */
    public int getEmpID()
    {
        return empID;
    }

    /**
     * @param empID the empID to set
     */
    public void setEmpID(int empID)
    {
        this.empID = empID;
    }

    /**
     * @return the servicedCustomer
     */
    public Customer getServicedCustomer()
    {
        return servicedCustomer;
    }

    /**
     * @param servicedCustomer the servicedCustomer to set
     */
    public void setServicedCustomer(Customer servicedCustomer)
    {
        this.servicedCustomer = servicedCustomer;
    }

    /**
     * @return the rentedCar
     */
    public Vehicle getRentedCar()
    {
        return rentedCar;
    }

    /**
     * @param rentedCar the rentedCar to set
     */
    public void setRentedCar(Vehicle rentedCar)
    {
        this.rentedCar = rentedCar;
    }
}