/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.problematica;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Milton_Leiva
 */
class Cliente {
 

// line 10 "problematica3.mp"


  //------------------------
  // MEMBER VARIABLES
  //------------------------

  //Cliente Attributes
  private int id_cliente;
  private String nombres;
  private String diu;
  private String barrio;

  //Cliente Associations
  private List<Medidor> medidors;

  //------------------------
  // CONSTRUCTOR
  //------------------------

  public Cliente(int aId_cliente, String aNombres, String aDiu, String aBarrio)
  {
    id_cliente = aId_cliente;
    nombres = aNombres;
    diu = aDiu;
    barrio = aBarrio;
  
    medidors = new ArrayList<Medidor>();
  }

  //------------------------
  // INTERFACE
  //------------------------

  public boolean setId_cliente(int aId_cliente)
  {
    boolean wasSet = false;
    id_cliente = aId_cliente;
    wasSet = true;
    return wasSet;
  }

  public boolean setNombres(String aNombres)
  {
    boolean wasSet = false;
    nombres = aNombres;
    wasSet = true;
    return wasSet;
  }

  public boolean setDiu(String aDiu)
  {
    boolean wasSet = false;
    diu = aDiu;
    wasSet = true;
    return wasSet;
  }

  public boolean setBarrio(String aBarrio)
  {
    boolean wasSet = false;
    barrio = aBarrio;
    wasSet = true;
    return wasSet;
  }

  public int getId_cliente()
  {
    return id_cliente;
  }

  public String getNombres()
  {
    return nombres;
  }

  public String getDiu()
  {
    return diu;
  }

  public String getBarrio()
  {
    return barrio;
  }
  /* Code from template association_GetOne */
  
  /* Code from template association_GetMany */
  public Medidor getMedidor(int index)
  {
    Medidor aMedidor = medidors.get(index);
    return aMedidor;
  }

  public List<Medidor> getMedidors()
  {
    List<Medidor> newMedidors = Collections.unmodifiableList(medidors);
    return newMedidors;
  }

  public int numberOfMedidors()
  {
    int number = medidors.size();
    return number;
  }

  public boolean hasMedidors()
  {
    boolean has = medidors.size() > 0;
    return has;
  }

  public int indexOfMedidor(Medidor aMedidor)
  {
    int index = medidors.indexOf(aMedidor);
    return index;
  }
  /* Code from template association_SetOneToMany */
  
  /* Code from template association_MinimumNumberOfMethod */
  public static int minimumNumberOfMedidors()
  {
    return 0;
  }
  /* Code from template association_AddManyToOne */
  public Medidor addMedidor(int aId_medidor, String aMarca, String aDireccion, double aCosto)
  {
    return new Medidor(aId_medidor, aMarca, aDireccion, aCosto,this);
  }

  public boolean addMedidor(Medidor aMedidor)
  {
    boolean wasAdded = false;
    if (medidors.contains(aMedidor)) { return false; }
    Cliente existingCliente = aMedidor.getCliente();
    boolean isNewCliente = existingCliente != null && !this.equals(existingCliente);
    if (isNewCliente)
    {
      aMedidor.setCliente(this);
    }
    else
    {
      medidors.add(aMedidor);
    }
    wasAdded = true;
    return wasAdded;
  }

  public boolean removeMedidor(Medidor aMedidor)
  {
    boolean wasRemoved = false;
    //Unable to remove aMedidor, as it must always have a cliente
    if (!this.equals(aMedidor.getCliente()))
    {
      medidors.remove(aMedidor);
      wasRemoved = true;
    }
    return wasRemoved;
  }
  /* Code from template association_AddIndexControlFunctions */
  public boolean addMedidorAt(Medidor aMedidor, int index)
  {  
    boolean wasAdded = false;
    if(addMedidor(aMedidor))
    {
      if(index < 0 ) { index = 0; }
      if(index > numberOfMedidors()) { index = numberOfMedidors() - 1; }
      medidors.remove(aMedidor);
      medidors.add(index, aMedidor);
      wasAdded = true;
    }
    return wasAdded;
  }

  public boolean addOrMoveMedidorAt(Medidor aMedidor, int index)
  {
    boolean wasAdded = false;
    if(medidors.contains(aMedidor))
    {
      if(index < 0 ) { index = 0; }
      if(index > numberOfMedidors()) { index = numberOfMedidors() - 1; }
      medidors.remove(aMedidor);
      medidors.add(index, aMedidor);
      wasAdded = true;
    } 
    else 
    {
      wasAdded = addMedidorAt(aMedidor, index);
    }
    return wasAdded;
  }

  @Override
  public String toString()
  {
    return  "["+
            "id_cliente" + ":" + getId_cliente()+ "," +
            "nombres" + ":" + getNombres()+ "," +
            "diu" + ":" + getDiu()+ "," +
            "barrio" + ":" + getBarrio()+ "]" + System.getProperties().getProperty("line.separator");
  }
}
    

