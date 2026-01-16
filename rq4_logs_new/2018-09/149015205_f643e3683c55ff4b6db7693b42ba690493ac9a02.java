
package com.tdp2.quechuaapp.model;

import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Curso {

    @SerializedName("id")
    @Expose
    public Integer id;
    @SerializedName("estado")
    @Expose
    public String estado;
    @SerializedName("vacantes")
    @Expose
    public Integer vacantes;
    @SerializedName("horarios")
    @Expose
    public List<Horario> horarios = null;
    @SerializedName("profesor")
    @Expose
    public Profesor profesor;
    @SerializedName("materia")
    @Expose
    public Materia materia;

}