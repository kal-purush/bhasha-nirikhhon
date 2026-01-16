/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package buildingcube;

import java.util.ArrayList;
import java.util.List;

import eu.printingin3d.javascad.models.Abstract3dModel;
import eu.printingin3d.javascad.coords.Angles3d;
import eu.printingin3d.javascad.models.Sphere;
import eu.printingin3d.javascad.coords.Coords3d;
import eu.printingin3d.javascad.coords.Dims3d;
import eu.printingin3d.javascad.models.*;
import eu.printingin3d.javascad.tranzitions.Difference;
import eu.printingin3d.javascad.tranzitions.Union;

class BuildingCube extends Union {
    private static final int myCoords1[] = {-133, -67, 0, 67, 133};
    private static final int myCoords2[] = {-80, -20, 40};
    private static int myCoords3 = -100;

    BuildingCube() { super(getModels()); }

    private static List<Abstract3dModel> getModels() {
        //полная модель
        List<Abstract3dModel> models = new ArrayList<Abstract3dModel>();

        Union first = new Union(
                new Cube(new Dims3d(300, 100, 300)).move(new Coords3d(-150, -550, 300)).rotate(Angles3d.yOnly(45)),
                new Cube(new Dims3d(1200, 400, 500)).move(Coords3d.zOnly(250)),
                new Cube(new Dims3d(1000, 300, 400)).move(new Coords3d(10, -150 , 180)),
                new Cube(new Dims3d(600, 600, 600)).move(new Coords3d(250, -100, 280)).rotate(new Angles3d(-10,10,28)),
                new Cube(new Dims3d(600, 400, 350)).move(new Coords3d(-40, -220, 350)).rotate(new Angles3d(0,-15,5)),
               new Cube(new Dims3d(130, 130, 150)).move(new Coords3d(-275, -355, 100)).rotate(new Angles3d(0,-15,5)),
               new Cube(new Dims3d(350, 40, 40)).move(new Coords3d(170, -470, 180)).rotate(Angles3d.zOnly(-30)),
               new Cube(new Dims3d(160, 40, 40)).move(new Coords3d(-60, -580, 180)).rotate(Angles3d.zOnly(30)),
               new Cube(new Dims3d(40, 40, 800)).move(new Coords3d(300, -520, 300)).rotate(Angles3d.xOnly(-8)),
               new Cube(new Dims3d(350, 40, 40)).move(new Coords3d(170, -450, 430)).rotate(Angles3d.zOnly(-30)),
               new Cube(new Dims3d(200, 40, 40)).move(new Coords3d(-60, -550, 430)).rotate(Angles3d.zOnly(30))
        );

        models.add(first);
        return models;
    }
}