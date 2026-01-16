package Map;

import Buildings.ResidentialTile;

public class Map
{

    private Terrain terrain[][];

    /* Map Constructor */
    public Map(String mapFileToParse){
        this.generateMapTerrain(mapFileToParse);
    }

    /*
    * generateMap(String mapFileToParse)
    * Takes a text file as a string (or other format) and generates the terrain layout.
    * May belong in IOModule - just have it here temporarily
    */
    private void generateMapTerrain(String mapFileToParse){
        // Based on Width and Height of map (extraced from file):
        terrain = new Terrain[0][0];
    }

    /*
    * setBuildingsOnMap()
    * Places (random? Simulated Annealing? Genetic?) urban layout buildings on the map. To be used/implemented in algorithms.
    */
    public void setBuildingsOnMap(){
        // Cannot build directly on a toxic waste site

        for(int row = 0; row < this.terrain.length; row++){
            for(int col = 0; col < this.terrain[row].length; col++){

                terrain[row][col].setBuilding(new ResidentialTile());

            }
        }

    }

    /*
    * getValueOfMap()
    * Returns the value of the map with the current urban layout.
    */
    public int getValueOfMap(){
        int mapValue = 0;

        //Iterate through every Terrain eleemnt of 2D array and check its value
        for(int row = 0; row < this.terrain.length; row++){
            for(int col = 0; col < this.terrain[row].length; col++){

                // Get the value of the current terrain and tile. Add to mapValue.
                mapValue += this.terrain[row][col].getValue(
                    getToxicNeighbors(row, col),
                    getScenicNeighbors(row, col),
                    getIndustrialNeighbors(row, col),
                    getResidentialNeighbors(row, col),
                    getCommercialNeighbors(row, col)
                );

            }
        }

        return mapValue;
    }

    /*
    * getToxicNeighbors(row,col)
    * Returns the number of Toxic terrains within 2 Manhattan Distances.
    */
    public int getToxicNeighbors(int row, int col){

        // Iterate trough the requisite terrain elements and check its type. Increment when Toxic.

        return 0;
    }

    /*
    * getScenicNeighbors(row,col)
    * Returns the number of Scenic terrains within 2 Manhattan Distances.
    */
    public int getScenicNeighbors(int row, int col){

        // Iterate trough the requisite terrain elements and check its type. Increment when Scenic.

        return 0;
    }

    /*
    * getIndustrialNeighbors(row,col)
    * Returns the number of Industrial Building Tiles within 2 Manhattan Distances.
    */
    public int getIndustrialNeighbors(int row, int col){

        // Iterate trough the requisite terrain elements and check the building type on it. Increment when Industrial.

        return 0;
    }

    /*
    * getResidentialNeighbors(row,col)
    * Returns the number of Residential Building Tiles within 2 Manhattan Distances.
    */
    public int getResidentialNeighbors(int row, int col){

        // Iterate trough the requisite terrain elements and check the building type on it. Increment when Residential.

        return 0;
    }

    /*
    * getCommercialNeighbors(row,col)
    * Returns the number of Commercial Building Tiles within 2 Manhattan Distances.
    */
    public int getCommercialNeighbors(int row, int col){

        // Iterate trough the requisite terrain elements and check the building type on it. Increment when Commercial.

        return 0;
    }

}