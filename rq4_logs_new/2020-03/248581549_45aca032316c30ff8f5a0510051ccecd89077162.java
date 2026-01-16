/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utng.bda.alumno;


/**
 *
 * @author benru
 */

public class Alumno {
    private  int idAlumno; //cambiarle nombres 
    private String nombreAlum;
    private String apPaterno;
    private String apMaterno;
    private String sexo;
    private String fecNacimiento;
    private double calif1; 
    private double calif2;  
    private double calif3; 
    private double calif4; 
    private double calif5;
    private int cuatrimestre;
    private Carrera carrera; 

    public Alumno(int idAlumno, String nombreAlum, String apPaterno, String apMaterno, String sexo, String fecNacimiento, double calif1, double calif2, double calif3, double calif4, double calif5, int cuatrimestre, Carrera carrera) {
        this.idAlumno = idAlumno;
        this.nombreAlum = nombreAlum;
        this.apPaterno = apPaterno;
        this.apMaterno = apMaterno;
        this.sexo = sexo;
        this.fecNacimiento = fecNacimiento;
        this.calif1 = calif1;
        this.calif2 = calif2;
        this.calif3 = calif3;
        this.calif4 = calif4;
        this.calif5 = calif5;
        this.cuatrimestre = cuatrimestre;
        this.carrera = carrera;
    }

    public Alumno() {
        this(0,"","","","","",0,0,0,0,0,0,null);
    }

    public int getIdAlumno() {
        return idAlumno;
    }

    public void setIdAlumno(int idAlumno) {
        this.idAlumno = idAlumno;
    }

    public String getNombreAlum() {
        return nombreAlum;
    }

    public void setNombreAlum(String nombreAlum) {
        this.nombreAlum = nombreAlum;
    }

    public String getApPaterno() {
        return apPaterno;
    }

    public void setApPaterno(String apPaterno) {
        this.apPaterno = apPaterno;
    }

    public String getApMaterno() {
        return apMaterno;
    }

    public void setApMaterno(String apMaterno) {
        this.apMaterno = apMaterno;
    }

    public String getSexo() {
        return sexo;
    }

    public void setSexo(String sexo) {
        this.sexo = sexo;
    }

    public String getFecNacimiento() {
        return fecNacimiento;
    }

    public void setFecNacimiento(String fecNacimiento) {
        this.fecNacimiento = fecNacimiento;
    }

    public double getCalif1() {
        return calif1;
    }

    public void setCalif1(double calif1) {
        this.calif1 = calif1;
    }

    public double getCalif2() {
        return calif2;
    }

    public void setCalif2(double calif2) {
        this.calif2 = calif2;
    }

    public double getCalif3() {
        return calif3;
    }

    public void setCalif3(double calif3) {
        this.calif3 = calif3;
    }

    public double getCalif4() {
        return calif4;
    }

    public void setCalif4(double calif4) {
        this.calif4 = calif4;
    }

    public double getCalif5() {
        return calif5;
    }

    public void setCalif5(double calif5) {
        this.calif5 = calif5;
    }

    public int getCuatrimestre() {
        return cuatrimestre;
    }

    public void setCuatrimestre(int cuatrimestre) {
        this.cuatrimestre = cuatrimestre;
    }

    public Carrera getCarrera() {
        return carrera;
    }

    public void setCarrera(Carrera carrera) {
        this.carrera = carrera;
    }

	@Override
	public String toString() {
		return "Alumno [idAlumno=" + idAlumno + ", nombreAlum=" + nombreAlum + ", apPaterno=" + apPaterno
				+ ", apMaterno=" + apMaterno + ", sexo=" + sexo + ", fecNacimiento=" + fecNacimiento + ", calif1="
				+ calif1 + ", calif2=" + calif2 + ", calif3=" + calif3 + ", calif4=" + calif4 + ", calif5=" + calif5
				+ ", cuatrimestre=" + cuatrimestre + ", carrera=" + carrera + "]";
	}
    
    

  
 
}
