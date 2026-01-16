package graph.path;

import java.io.*;

/**
 * Write a program that reads in a file representing a connected undirected weighted
 * graph in the form of an adjacency matrix and outputs the longest path between any
 * two vertices in the graph.
 * For example, if you load in the file graph.txt the output should look something
 * like this:
 * 20
 * EBCJ
 * This is a difficult programming challenge. This code might help you make a start if
 * you want to use it:
 */
public class Graph{
    public static void main(String[] args){

        FileIO io = new FileIO();
        String[] original = io.load("src" + File.separator + "graph" + File.separator +"path" + File.separator + "Graph.txt");
        int size=original.length-1;
        int[][] array = new int[size][size];

        for(int i=1;i<=size;i++){ //load up the data into array
            for(int j=1;j<=size;j++){
                array[i-1][j-1]=Integer.parseInt(original[i].split("\t")[j]);
            }
        }


        int furthestdistance=0; //keep track of the longest path found so far
        String furthestroute="";
//       for(int i=0;i<size;i++){ //loop through all possible starting positions: start from A
            int i=2;
            int visited=0; //how many vertices have been visited so far?
            Vertex[] vertices = new Vertex[size]; //initiate the vertex objects
            for(int j=0;j<size;j++){
                vertices[j]=new Vertex(original[0].split("\t")[j]);
            }
            vertices[i].visited=true; //we visit the starting position
            visited++; //we've visited one vertex so far
            vertices[i].distance=0; //set this vertex as starting point

            for(int j=0;j<size;j++){ //update distances from starting point
                if(array[j][i]>0){ //only update those that can be reached from here
                    vertices[j].distance=array[j][i];
                    System.out.println(j+" "+i+" "+array[j][i]);
                }
            }


        int index=0;
        for(Vertex v:vertices){
            if(v.distance>0){

            }
            index++;
        }



        int t2=i-1;
        while(t2>=0) {
            for(int j = 0; j < size; j++) {//0-7
                System.out.println("j:" + j);
                System.out.println("t:" + t2);
                System.out.println(array[j][t2]);
                System.out.println(vertices[j].visited);


                    if((array[j][t2] > 0) && (vertices[j].visited == false)) { //get all vertrices connect to B , except those already from already visited vertices.
                        System.out.println("singledis " + vertices[j].distance);
                        int distance = array[j][t2] + vertices[j].distance;
                        System.out.println("totaldis " + distance);
                        if(distance > vertices[t2].distance) {   //update this vertices to hold the longgest distance and route;
                            vertices[t2].distance = distance;
                            vertices[t2].route = j;
                            System.out.println("set vertices" + t2 + " route:" + j + " distance:" + distance);
                        }
                    }



            }
            vertices[t2].visited = true;
            visited++;
            t2--;
            System.out.println("----------------------");
        }

        int t=i;//i =0;

            while(visited<size) { //while not all vertices visited …fill this in


                    for(int j=0;j<size;j++){//0-7
                        System.out.println("j:"+j);
                        System.out.println("t:"+t);
                        System.out.println(array[j][t]);
                        System.out.println(vertices[j].visited);

                            if((array[j][t]>0)&&(vertices[j].visited==false)){ //get all vertrices connect to B , except those already from already visited vertices.
                                System.out.println("singledis "+vertices[j].distance);
                                int distance =array[j][t]+vertices[t].distance;
                                System.out.println("totaldis "+distance);
                                if(distance > vertices[j].distance){   //update this vertices to hold the longgest distance and route;
                                    vertices[j].distance=array[j][t]+vertices[t].distance;
                                    vertices[j].route = t;
                                    System.out.println("set vertices"+j+" route:"+t+" singledistance:"+array[j][t]);
                                }
                            }



                    }
                    vertices[t].visited=true;
                    visited++;
                    t++;
                System.out.println("---");
                System.out.println(visited<size);



            }



        printVertices(vertices);

        //A0,B1,C2,D4,E6,F3,G7,H5

       // vertices[0].label ="A";
       // vertices[0].route =-1;
       // vertices[0].distance=0;

       // vertices[1].label ="B";
      //  vertices[1].route =-1;
       // vertices[1].distance=3;
       // vertices[2].label ="C";
       // vertices[2].route =-1;
       // vertices[2].distance=6;

//        vertices[3].label ="D";
//        vertices[3].route =2;
//        vertices[3].distance=11;
//        vertices[4].label ="E";
//        vertices[4].route =3;
//        vertices[4].distance=17;
//        vertices[5].label ="F";
//        vertices[5].route =2;
//        vertices[5].distance=8;
//        vertices[6].label ="G";
//        vertices[6].route =4;
//        vertices[6].distance=21;
//        vertices[7].label ="H";
//        vertices[7].route =5;
//        vertices[7].distance=17;

//            if(i>1){
//                vertices[i].distance = max+vertices[vertices[i-1].route].distance;
//                System.out.println("-----");
//                System.out.println(vertices[i].label);
//                System.out.println(vertices[vertices[i].route].label);
//                System.out.println(vertices[i].distance);
//                System.out.println("-----");
//            }






            for(int x=0;x<size;x++){ //check for a new record distance, size: number of vertices
                if(vertices[x].distance>furthestdistance){ //go through all distances in the graph
                    furthestdistance=vertices[x].distance;
                    int visiting=x;
                    String solution=""; //build up the path taken to get this particular record beating distance
                    while(visiting>=0){ //the starting position has route of -1
                        solution=vertices[visiting].label+""+solution;
                        visiting=vertices[visiting].route; //step back along the route taken to generate this distance
                    }
                    solution=vertices[i].label+""+solution; //this is the starting position
                    furthestroute=solution; //keep track of this record path
                }
            }
//           System.out.println(furthestdistance); //print out the results
//           System.out.println(furthestroute);

//        }
        System.out.println(furthestdistance); //print out the results
        System.out.println(furthestroute);
    }

    public static void printVertices(Vertex vertices[]){
        //print vertices
        for(int a=0;a<vertices.length;a++){
            System.out.println("vertice"+a+" "+vertices[a].label+ " route"+vertices[a].route+" distance"+vertices[a].distance);
        }
    }



}
class Vertex{ //everything you need for a vertex
    public boolean visited=false; //has it been visited?
    int distance=Integer.MIN_VALUE; //the shortest route from starting position to this vertex
    int route=-1; //keep track of the last step taken to reach this vertex for the shortest path - what vertex did it come from?
    String label; //name of the vertex

    public Vertex (String labelin){ //give the vertex a name when instantiated
                label=labelin;
    }
}

class FileIO {

    public String[] load(String file) {
        File aFile = new File(file);
        StringBuilder contents = new StringBuilder();
        BufferedReader input = null;
        try {
            input = new BufferedReader(new FileReader(aFile));
            String line ;
            int i = 0;
            while((line = input.readLine()) != null) {
                contents.append(line);
                i=i+1;
                contents.append(System.getProperty("line.separator"));
            }
        } catch(FileNotFoundException ex) {
            System.out.println("Can't find the file - are you sure the file is in this location: " + file);
            ex.printStackTrace();
        } catch(IOException ex) {
            System.out.println("Input output exception while processing file");
            ex.printStackTrace();
        } finally {
            try {
                if(input != null) {
                    input.close();
                }
            } catch(IOException ex) {
                System.out.println("Input output exception while processing file");
                ex.printStackTrace();
            }
        }
        String[] array = contents.toString().split("\n");
        for(int i = 0; i < array.length; i++) {
            array[i] = array[i].trim();
        }
        return array;
    }

    public void save(String file, String[] array) throws IOException {
        File aFile = new File(file);
        Writer output = null;
        try {
            output = new BufferedWriter(new FileWriter(aFile));

            for(String s:array){
                output.write(s);
                output.write(System.getProperty("line.separator"));
            }
        } finally {
            if(output != null) output.close();
        }
    }


}