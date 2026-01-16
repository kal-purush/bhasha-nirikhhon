public class TestReadWrite
{
    public static void main(String args[])
    {
        National nat = new National();
        nat.read("NetballWA.csv");

        System.out.println();
        System.out.println("Testing read in from \"NetballWA.csv\"");
        System.out.println("------------");
        System.out.println(nat.getName());
        for(State s : nat.getStates())
        {
            System.out.println("|  |"+s.getName());
            for(Association a : s.getAssociations())
            {
                System.out.println("|  |  |"+a.getName());
                for(Club c : a.getClubs())
                {
                    System.out.println("|  |  |  |"+c.getName());
                    for(Team t : c.getTeams())
                    {
                        System.out.println("|  |  |  |  |"+t.getName());
                    }
                }
            }
        }

        System.out.println();
        System.out.println("Testing write to \"TestOutputFile.csv\"");
        System.out.println("-------------");
        nat.write("TestOutputFile.csv");
    }

}