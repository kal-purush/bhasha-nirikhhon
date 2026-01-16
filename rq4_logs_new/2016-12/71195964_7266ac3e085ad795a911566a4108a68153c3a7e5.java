package com.proyecto.rutaschool.datoFirebase;

/**
 * Created by Juan David on 06/12/2016.
 */

public class Usuario {
    String name;
    String dir;

    public Usuario() {
    }

    public Usuario(String name, String dir) {
        this.name = name;
        this.dir = dir;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }
}