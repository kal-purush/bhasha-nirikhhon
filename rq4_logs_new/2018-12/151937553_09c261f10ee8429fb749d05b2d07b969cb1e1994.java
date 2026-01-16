package com.company;

import com.company.buildings.dwelling.DwellingFactory;
import com.company.buildings.office.Office;
import com.company.buildings.office.OfficeBuilding;
import com.company.buildings.office.OfficeFloor;
import com.company.interfaces.Building;
import com.company.interfaces.BuildingFactory;
import com.company.interfaces.Floor;
import com.company.interfaces.Space;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Scanner;

public class Buildings {

    private static BuildingFactory factory = new DwellingFactory();

    public static void setBuildingFactory(BuildingFactory factory){
        Buildings.factory = factory;
    }

    public static Space createSpace(double square) {
        return factory.createSpace(square);
    }

    public static Space createSpace(double square, int amountRooms) {
        return factory.createSpace(amountRooms,square);
    }

    public static Floor createFloor(int amountSpaces) {
        return factory.createFloor(amountSpaces);
    }

    public static Floor createFloor(Space... spaces) {
        return factory.createFloor(spaces);
    }

    public static Building createBuilding(int amountFloors, int... amountSpaces) {
        return factory.createBuilding(amountFloors, amountSpaces);
    }

    public static Building createBuilding(Floor... floors) {
        return factory.createBuilding(floors);
    }








    public static void outputBuilding (Building building, OutputStream out) throws IOException {
//        building.getClass().getName();
        out.write(ByteBuffer.allocate(4).putInt(building.getNumFloors()).array());
        for(Floor floor: building.getFloors()) {
            out.write(ByteBuffer.allocate(4).putInt(floor.size()).array());
            for(Space space : floor.getSpaces()) {
                out.write(ByteBuffer.allocate(4).putInt(space.getRooms()).array());
                out.write(ByteBuffer.allocate(8).putDouble(space.getSquare()).array());
            }
        }
    }

    public static Building inputBuilding (InputStream in) throws IOException {
        byte[] bytes = new byte[4];
        byte[] bytesD = new byte[8];
        in.read(bytes);
        Floor[] floors = new Floor[ByteBuffer.wrap(bytes).getInt()];

        for(int i = 0; i < floors.length; i++){
            in.read(bytes);
            Space[] spaces = new Space[ByteBuffer.wrap(bytes).getInt()];

            for(int j = 0; j < spaces.length; j++) {
                in.read(bytes);
                int amountRooms = ByteBuffer.wrap(bytes).getInt();

                in.read(bytesD);
                double square = ByteBuffer.wrap(bytesD).getDouble();

//                spaces[j] = new Office(square,amountRooms);
                spaces[j] =  createSpace(square,amountRooms);
            }

            floors[i] = createFloor(spaces);
        }
        return createBuilding(floors);
    }


//сначала собрать билдером потом write
    public static void writeBuilding (Building building, Writer out) throws IOException {
        String str = "";
        StringBuffer strBuffer = new StringBuffer(str);

        strBuffer.append(building.getNumFloors() + " ");

//        out.write(building.getNumFloors() + " ");

        for(Floor floor: building.getFloors()) {
            strBuffer.append(floor.size() + " ");
//            out.write(floor.size() + " ");

            for(Space space : floor.getSpaces()) {
//                out.write(space.getRooms() + " ");
                strBuffer.append(space.getRooms() + " ");
//                out.write(space.getSquare() + " ");
                strBuffer.append(space.getSquare() + " ");
            }
        }
        strBuffer.append("\n");
//        out.write("\n");
        out.write(strBuffer.toString());
    }


    public static Building readBuilding (Reader in) throws IOException {
        StreamTokenizer st = new StreamTokenizer(in);

        st.nextToken();
        Floor[] floors = new Floor[(int)st.nval];

        for(int i = 0; i < floors.length; i++){
            st.nextToken();
            Space[] spaces = new Space[(int)st.nval];

            for(int j = 0; j < spaces.length; j++) {
                st.nextToken();
                int amountRooms = (int)st.nval;

                st.nextToken();
                double square = st.nval;

                spaces[j] = createSpace(square, amountRooms);
            }

            floors[i] = createFloor(spaces);
        }
        return createBuilding(floors);
    }


    public static void serializeBuilding (Building building, OutputStream out) throws IOException {
        (new ObjectOutputStream(out)).writeObject(building);
    }

    public static Building deserialaizeBuilding (InputStream in) throws IOException, ClassNotFoundException {
        return (Building) (new ObjectInputStream(in)).readObject();
    }



    public static void writeBuildingFormat (Building building, Writer out) throws IOException {
        PrintWriter writer = new PrintWriter(out);
        writer.printf("Здание (Кол-во этажей = %d)\n", building.getNumFloors());

        int i = 1;
        int j = 1;
        for(Floor floor: building.getFloors()) {
            writer.printf("\tЭтаж №%d (Кол-во помещений = %d)\n", i, floor.size());

            for(Space space : floor.getSpaces()) {
                writer.printf("\t\tПомещение №%d: Кол-во комнат = %d; Площадь = %f\n",
                        j, space.getRooms(), space.getSquare());
                j++;
            }
            i++;
        }
    }

    public static Building readBuilding(Scanner in){
        in.useDelimiter("[); \n]");

        Floor[] floors = new Floor[0];

        while(in.hasNext())
            if(in.hasNextInt()) {
                floors = new Floor[in.nextInt()];
                break;
            }
            else in.next();

        for(int i = 0; i < floors.length; i++){
            Space[] spaces = new Space[0];

            while(in.hasNext())
                if(in.hasNextInt()) {
                    spaces = new Space[in.nextInt()];
                    break;
                }
                else in.next();

            for(int j = 0; j < spaces.length; j++) {
                int amountRooms = 0;

                while(in.hasNext())
                    if(in.hasNextInt()) {
                        amountRooms = in.nextInt();
                        break;
                    }
                    else in.next();

                double square = 0;

                while(in.hasNext())
                    if(in.hasNextDouble()) {
                        square = in.nextDouble();
                        break;
                    }
                    else in.next();

                spaces[j] = createSpace(square, amountRooms);
            }
            floors[i] = createFloor(spaces);
        }
        return createBuilding(floors);
    }

    public static <T extends Comparable<T>> void sortUp(T[] objects) {
        for (int i = 0; i < objects.length; i++) {
            int minIndex = i;
            for (int j = i + 1; j < objects.length; j++) {
                if (objects[j].compareTo(objects[minIndex]) < 0)
                    minIndex = j;
            }
            T swapBuf = objects[i];
            objects[i] = objects[minIndex];
            objects[minIndex] = swapBuf;
        }
    }

    public static <T > void sortDown(Comparator<T> comparators,T[] objects) {
        for (int i = 0; i < objects.length; i++) {
            int minIndex = i;
            for (int j = i + 1; j < objects.length; j++) {
                if (comparators.compare(objects[j],objects[minIndex]) > 0)
                    minIndex = j;
            }
            T swapBuf = objects[i];
            objects[i] = objects[minIndex];
            objects[minIndex] = swapBuf;
        }
    }

}