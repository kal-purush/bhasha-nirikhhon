package javascadbuilding;

import eu.printingin3d.javascad.coords.Angles3d;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.printingin3d.javascad.coords.Coords3d;
import eu.printingin3d.javascad.coords.Dims3d;
import eu.printingin3d.javascad.enums.Side;
import eu.printingin3d.javascad.models.Abstract3dModel;
import eu.printingin3d.javascad.models.Cube;
import eu.printingin3d.javascad.models.Cylinder;
import eu.printingin3d.javascad.tranzitions.Difference;
import eu.printingin3d.javascad.tranzitions.Union;

public class Building extends Union
{
    Building()
    {
        super(getModels());
    }
    
    private static List<Abstract3dModel> getModels()
    {
        List<Abstract3dModel> models = new ArrayList();
        
        Union Foundation = new Union(
                
                new Cube(new Dims3d(2000, 1100, 1600)).move(new Coords3d(0, -550, 800)),
                new Cube(new Dims3d(2000, 600, 3500)).move(new Coords3d(0, 300, 1750)),
                new Cube(new Dims3d(1500, 750, 2900)).move(new Coords3d(150, 950, 1450)),
                new Cube(new Dims3d(1900, 1500, 1600)).move(new Coords3d(-50, 2075, 800)),
                new Cube(new Dims3d(2000, 1100, 1000)).move(new Coords3d(0, -550, 2100)),
                new Cube(new Dims3d(700, 1100, 100)).move(new Coords3d(-1300, -550, 1400)),
                new Cube(new Dims3d(1100, 200, 200)).move(new Coords3d(0, -100, 3000)),
                new Cube(new Dims3d(200, 200, 300)).move(new Coords3d(-500, 300, 3600)),
                new Cube(new Dims3d(200, 500, 200)).move(new Coords3d(-900, -850, 2700)),
                new Cube(new Dims3d(200, 500, 400)).move(new Coords3d(-900, -400, 2800)).rotate(new Angles3d(0, 0, 90))   
        );              
        
        Difference DiffFound = new Difference(
                Foundation,
                new Cube(new Dims3d(200, 150, 300)).move(new Coords3d(900, -1040, 150)),
                new Cube(new Dims3d(200, 900, 950)).move(new Coords3d(900, -500, 1000)),
                new Cube(new Dims3d(200, 1050, 350)).move(new Coords3d(900, -445, 175)),
                new Cube(new Dims3d(200, 1100, 300)).move(new Coords3d(900, 600, 150)),
                new Cube(new Dims3d(100, 250, 2800)).move(new Coords3d(980, 300, 1900)),
                new Cube(new Dims3d(50, 600, 450)).move(new Coords3d(900, 900, 2500)),
                new Cube(new Dims3d(2000, 600, 600)).move(new Coords3d(0, 900, 1900)),
                new Cube(new Dims3d(2000, 1100, 600)).move(new Coords3d(200, -550, 2100)),
                new Cube(new Dims3d(800, 400, 600)).move(new Coords3d(900, -200, 2300)),
                new Cube(new Dims3d(1500, 600, 300)).move(new Coords3d(1100, 900, 1200)),
                new Cube(new Dims3d(1500, 600, 300)).move(new Coords3d(1100, 900, 650)),
                new Cube(new Dims3d(1500, 1500, 300)).move(new Coords3d(1500, 2070, 1200)),
                new Cube(new Dims3d(1500, 1500, 300)).move(new Coords3d(1500, 2070, 650)),
                new Cube(new Dims3d(1500, 1500, 300)).move(new Coords3d(1500, 2070, 150)),
                new Cube(new Dims3d(700, 400, 300)).move(new Coords3d(600, 2770, 1200)),
                new Cube(new Dims3d(700, 400, 300)).move(new Coords3d(600, 2770, 650)),
                new Cube(new Dims3d(700, 400, 300)).move(new Coords3d(600, 2770, 150)),
                new Cube(new Dims3d(900, 200, 200)).move(new Coords3d(0, -100, 2900)),
                new Cube(new Dims3d(200, 500, 200)).move(new Coords3d(-900, -50, 2700)).rotate(new Angles3d(0, 0, 90))
        );
        
        models.add(DiffFound);
        
        return models;
    }
    
}