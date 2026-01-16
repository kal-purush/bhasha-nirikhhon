package com.example.emergensui.automotive_ui;

public class Doctor {
    private String medical_degree;
    private String name;
    private String spec_id;

    //Constructors
    public Doctor(){}
    public Doctor(String name, String medical_degree, String spec_id)
    {
        this.name = name;
        this.medical_degree = medical_degree;
        this.spec_id = spec_id;
    }

    public String getName()
    {
        return name;
    }

    public String getMedical_degree()
    {
        return medical_degree;
    }

    public String getSpec_id()
    {
        return spec_id;
    }


    public void setName(String name)
    {
        this.name = name;
    }

    public void setMedical_degree(String medical_degree)
    {
        this.medical_degree = medical_degree;
    }

    public void setSpec_id(String spec_id)
    {
        this.spec_id = spec_id;
    }

}