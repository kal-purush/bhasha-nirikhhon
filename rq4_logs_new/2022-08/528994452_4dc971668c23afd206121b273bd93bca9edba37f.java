package com.example.sistema.Entities;

public class Empleado {
    private String nombreEmpelado;
    private String email;
    private String empresaPertenece;
    private String rol;

    public Empleado(String nombreEmpelado, String email, String empresaPertenece, String rol) {
        this.nombreEmpelado = nombreEmpelado;
        this.email = email;
        this.empresaPertenece = empresaPertenece;
        this.rol = rol;
    }

    public String getNombreEmpelado() {
        return nombreEmpelado;
    }

    public void setNombreEmpelado(String nombreEmpelado) {
        this.nombreEmpelado = nombreEmpelado;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getEmpresaPertenece() {
        return empresaPertenece;
    }

    public void setEmpresaPertenece(String empresaPertenece) {
        this.empresaPertenece = empresaPertenece;
    }

    public String getRol() {
        return rol;
    }

    public void setRol(String rol) {
        this.rol = rol;
    }
}