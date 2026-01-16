package tec.poo.tareas;

public class Empleado 
{
    private String Nombre;
    private double SalarioBase;

    public Empleado()
    {
        this.Nombre = Nombre;
        this. SalarioBase = SalarioBase;
    }

    public String GetNombre()
    {
        return this.Nombre;
    }

    public void setNombre(String Nombre)
    {
        this.Nombre = Nombre;
    }
    public double GetSalarioBase()
    {
        return this.SalarioBase;
    }

    public void setSalarioBase(double SalarioBase)
    {
        this.SalarioBase = SalarioBase;
    }
    public String toString()
    {
        return "\nNombre: " + this.GetNombre() + "\nSalario base: " + this.GetSalarioBase();
    }
    public double CalcularSalario(double base)
    {
        return base * 2;
    }
    
}