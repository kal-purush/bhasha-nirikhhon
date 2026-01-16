package com.building.gr151l5st19;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.printingin3d.javascad.coords.Angles3d;
import eu.printingin3d.javascad.models.Abstract3dModel;
import eu.printingin3d.javascad.coords.Coords3d;
import eu.printingin3d.javascad.coords.Dims3d;
import eu.printingin3d.javascad.models.*;
import eu.printingin3d.javascad.tranzitions.Difference;
import eu.printingin3d.javascad.tranzitions.Union;

public class Building extends Union {
    	private static final int windcoord = -3;
	
	public Building() {
		super(getModels());
	}



	private static List<Abstract3dModel> getModels() {
		List<Abstract3dModel> models = new ArrayList();
          
		//Главный блок
                List<Abstract3dModel> windows = new ArrayList<Abstract3dModel>();
                for (int i = windcoord; i <= 13; i += 13) {
                   windows.add(new Cube(new Dims3d(32, 4, 8)).move(new Coords3d(45, -20, i)));
                   windows.add(new Cube(new Dims3d(32, 4, 8)).move(new Coords3d(45, -12, i)));
                   windows.add(new Cube(new Dims3d(32, 4, 8)).move(new Coords3d(45, -4, i)));
                   windows.add(new Cube(new Dims3d(32, 4, 8)).move(new Coords3d(45, 4, i)));
                   windows.add(new Cube(new Dims3d(32, 4, 8)).move(new Coords3d(45, 12, i)));
                   windows.add(new Cube(new Dims3d(32, 4, 8)).move(new Coords3d(45, 20, i)));
                }     
               
                Union windows_all = new Union(
                       windows
                );
                
		Difference Main_Build = new Difference(
                   new Cube(new Dims3d(70, 70, 40)),
                   //окна
                   windows_all,
                   windows_all.rotate(new Angles3d(0,0,90)),
                   windows_all.rotate(new Angles3d(0,0,-90)),
                   windows_all.move(new Coords3d(-90, 0, 0)),
                   //двери
                   new Cylinder(21,2.4).rotate(new Angles3d(0,90,0)).move(new Coords3d(-25-19, -20, -12)),  
                   new Cube(new Dims3d(21, 4.8, 8)).move(new Coords3d(-25-19, -20, -15.5)),
                   new Cube(new Dims3d(28, 4, 8)).move(new Coords3d(-25-19, -20, -15.5)),
                   //нижние окна
                   new Cylinder(30,2.4).rotate(new Angles3d(0,90,0)).move(new Coords3d(-25-19, 15, -12)),  
                   new Cube(new Dims3d(30, 4.8, 5)).move(new Coords3d(-25-19, 15, -15)),
                   new Cylinder(30,2.4).rotate(new Angles3d(0,90,0)).move(new Coords3d(-25-19, 5, -12)),  
                   new Cube(new Dims3d(30, 4.8, 5)).move(new Coords3d(-25-19, 5, -15))
                                    
		);
             
                
                //Башни
                Union Tower = new Union(
                   //Основание
                   new Cylinder(55,10).move(new Coords3d(-35, -35, 7.5)),
                   new Cylinder(55,10).move(new Coords3d(35, 35, 7.5)), 
                   new Cylinder(55,10).move(new Coords3d(35, -35, 7.5)), 
                   new Cylinder(55,10).move(new Coords3d(-35, 35, 7.5)),
                   //Верхняя часть
                   new Cylinder(10,11).move(new Coords3d(-35, -35, 35)),
                   new Cylinder(10,11).move(new Coords3d(35, 35, 35)), 
                   new Cylinder(10,11).move(new Coords3d(35, -35, 35)), 
                   new Cylinder(10,11).move(new Coords3d(-35, 35, 35)),
                   //Крыша
                   new Cylinder(10,13,0.5).move(new Coords3d(-35, -35, 45)),
                   new Cylinder(10,13,0.5).move(new Coords3d(35, 35, 45)), 
                   new Cylinder(10,13,0.5).move(new Coords3d(35, -35, 45)), 
                   new Cylinder(10,13,0.5).move(new Coords3d(-35, 35, 45)),
                   //Наконечники
                   new Cylinder(30,0.5).move(new Coords3d(-35, -35, 50)),
                   new Cylinder(30,0.5).move(new Coords3d(35, 35, 50)), 
                   new Cylinder(30,0.5).move(new Coords3d(35, -35, 50)), 
                   new Cylinder(30,0.5).move(new Coords3d(-35, 35, 50)),
                   //фундамент
                   new Cube(new Dims3d(100, 100, 2)).move(new Coords3d(0, 0, -21))
                );

                Union Window_Frame = new Union(Tower,
                   new Cube(new Dims3d(12.5, 5, 8)).move(new Coords3d(40, -35, 15)),
                   new Cube(new Dims3d(12.5, 5, 8)).move(new Coords3d(-40, -35, 15)),
                   new Cube(new Dims3d(12.5, 5, 8)).move(new Coords3d(40, 35, 15)),
                   new Cube(new Dims3d(12.5, 5, 8)).move(new Coords3d(-40, 35, 15))
                );
                
                Difference Tower_Windows = new Difference(Window_Frame,
                   new Cube(new Dims3d(13, 3, 6)).move(new Coords3d(52, -35, 15)),
                   new Cube(new Dims3d(13, 3, 6)).move(new Coords3d(-52, -35, 15)),  
                   new Cube(new Dims3d(13, 3, 6)).move(new Coords3d(52, 35, 15)),   
                   new Cube(new Dims3d(13, 3, 6)).move(new Coords3d(-52, 35, 15))   
               
               );
                
          

               
                //Крыша
                Union Roof = new Union(
                    new Prism(15, 50, 30, 4).rotate(Angles3d.zOnly(45)).move(Coords3d.zOnly(10+18)),
	            new Cube(new Dims3d(72, 72, 2)).move(Coords3d.zOnly(20)),
                    new Cube(new Dims3d(42, 42, 2)).move(Coords3d.zOnly(35)),
                    new Cube(new Dims3d(3, 3, 15)).move(new Coords3d(-25, -10, 35)),
                    new Cube(new Dims3d(3, 3, 15)).move(new Coords3d(25, -10, 35)),
                    new Cube(new Dims3d(3, 10, 15)).move(new Coords3d(0, 20, 35)),
                    new Cube(new Dims3d(2, 2, 15)).move(new Coords3d(0, 22, 40)),
                    new Cube(new Dims3d(2, 2, 15)).move(new Coords3d(0, 17, 40))            
		);
 
                
                //Окна на крыше
                Difference Roof_Window = new Difference(
                    new Cube(new Dims3d(8, 6, 4)).move(Coords3d.xOnly(-30)).move(Coords3d.zOnly(50)),    
                    new Cube(new Dims3d(10, 5, 10)).rotate(new Angles3d(45,0,0)).move(Coords3d.yOnly(5)).move(Coords3d.xOnly(-30)).move(Coords3d.zOnly(50)),
                    new Cube(new Dims3d(10, 5, 10)).rotate(new Angles3d(-45,0,0)).move(Coords3d.yOnly(-5)).move(Coords3d.xOnly(-30)).move(Coords3d.zOnly(50))        
                );
                
                Difference Roof_Window1 = new Difference(
                    new Cube(new Dims3d(7.5, 5.5, 6)).move(Coords3d.xOnly(-30)).move(Coords3d.zOnly(45)),    
                    new Cube(new Dims3d(6, 4, 5)).move(Coords3d.xOnly(-35)).move(Coords3d.zOnly(45))        
                );
                
		models.add(Main_Build);
                models.add(Roof);
                models.add(Roof_Window.move(new Coords3d(0, 0, -19.5)));
                models.add(Roof_Window1.move(new Coords3d(0, 0, -19.5)));
                models.add(Roof_Window.move(new Coords3d(0, 20, -19.5)));
                models.add(Roof_Window1.move(new Coords3d(0, 20, -19.5)));
                models.add(Roof_Window.move(new Coords3d(0, -20, -19.5)));
                models.add(Roof_Window1.move(new Coords3d(0, -20, -19.5)));
                models.add(Roof_Window.rotate(new Angles3d(0,0,180)).move(new Coords3d(0, 0, -19.5)));
                models.add(Roof_Window1.rotate(new Angles3d(0,0,180)).move(new Coords3d(0, 0, -19.5)));
                models.add(Roof_Window.rotate(new Angles3d(0,0,180)).move(new Coords3d(0, 20, -19.5)));
                models.add(Roof_Window1.rotate(new Angles3d(0,0,180)).move(new Coords3d(0, 20, -19.5)));
                models.add(Roof_Window.rotate(new Angles3d(0,0,180)).move(new Coords3d(0, -20, -19.5)));
                models.add(Roof_Window1.rotate(new Angles3d(0,0,180)).move(new Coords3d(0, -20, -19.5)));
                models.add(Tower);
                models.add(Tower_Windows);
		return models;
	}

}