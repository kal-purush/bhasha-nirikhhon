package tec.poo.tareas.registroempleados.employeeregistry;
import java.util.Scanner;

public class Employee {
    String nombre;
    int horas_trabajadas;
    float tarifa_por_hora;
    float salario_base;
    boolean jornada;
    // Constructor 
    public Employee(String nombre, int horas_trabajadas, float tarifa_por_hora, float salario_base, boolean jornada) {
        this.nombre = nombre;
        this.horas_trabajadas = horas_trabajadas;
        this.tarifa_por_hora = tarifa_por_hora;
        this.salario_base = salario_base;
        this.jornada = jornada;
    }
    
    public static Employee agregarEmpleado() {
        Scanner entrada = new Scanner(System.in);
        
        System.out.println("Indique el nombre:");
        String nombre = entrada.nextLine();
        
        System.out.println("Indique las horas trabajadas:");
        int horas_trabajadas = entrada.nextInt();
        
        boolean jornada = determinar_jornada(horas_trabajadas);

        System.out.println("Indique la tarifa por hora:");
        float tarifa_por_hora = entrada.nextFloat();
        
        float salario_base = calcular_salario(tarifa_por_hora, horas_trabajadas);
        
        Employee nuevoEmpleado = new Employee(nombre, horas_trabajadas, tarifa_por_hora, salario_base,jornada);
        
        return nuevoEmpleado;
    }

    public static float calcular_salario(float tarifa_por_hora, int horas_trabajadas ){
        float salario_base = tarifa_por_hora * horas_trabajadas * 20;
        return salario_base;
    }

    public static boolean determinar_jornada(int horas_trabajadas){
        boolean jornada;
        if(horas_trabajadas>=8){
            jornada = true;
        }else{
            jornada = false;
        }
        return jornada;

    }
}