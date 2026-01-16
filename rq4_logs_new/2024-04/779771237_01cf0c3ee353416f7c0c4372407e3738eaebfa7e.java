package compiler;

/**
 * This class represents a KnightCode variable to be stored in the symbol table.
 * Supports only Strings and Integers
 * 
 * @author Kirin Sharma
 * @version 1.0
 * Assignment 5
 * CS322 - Compiler Construction
 * Spring 2024
 */

public class Variable<T>
{

    public T data; // the value of the variable
    public String dataType; // the data type of the variable
    public int memoryLocation;

    /**
     * Preferred constructor initializes the data to null
     * @param data
     */
    public Variable(String type)
    {
        data = null;
        dataType = type;
    } // end preferred constructor

    /**
     * Setter for data
     * @param data
     */
    public void setData(T data)
    {
        this.data = data;
    } // end setData

    /**
     * Getter for data
     * @return
     */
    public T getData()
    {
        return data;
    } // end getData

    /**
     * Getter for dataType
     * @return
     */
    public String getDataType()
    {
        return dataType;
    } // end getDataType

    /**
     * Setter for memoryLocation
     * @param location
     */
    public void setMemoryLocation(int location)
    {
        memoryLocation = location;
    } // end setMemoryLocation

    /**
     * Getter for memoryLocation
     * @return
     */
    public int getMemoryLocation()
    {
        return memoryLocation;
    } // end getMemoryLocation

} // end class