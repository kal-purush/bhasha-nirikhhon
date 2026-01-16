package MapGenerator.MapGenerator;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        Generator aGen = new Generator();
        aGen.execute();
        int[][] theMap = aGen.mapInt;
        //TileType[][] theMap = aGen.execute();
        String theSMap = "";
        for(int i = 0; i < theMap.length; ++i){
        	for(int j = 0; j < theMap[i].length; ++j){
        		theSMap += theMap[i][j]+" ";
        	}
        	theSMap += "\n";
        }
        System.out.println(theSMap);
    }
}