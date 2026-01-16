package oop.seminar3.task1;

public class Program {
    public static void main(String[] args) {
        Father f1 = new Father( "Father_1", 55 );
        Children c1 = new Children( "Children_1", 18 );
        Husband h1 = new Husband( "Husband_1", 40 );
        Wife w1 = new Wife( "Wife_1", 30 );

        Repository<Person> repo = new Repository<>( "repo_1" );
        repo.append( f1, Relationships.FATHER, c1 );
        repo.append( h1, Relationships.HUSBANT, w1 );
        System.out.println(repo);

        Research research =  new Research(repo);
        System.out.println(research.findChildren( f1 ));
        System.out.println(research.findFather( c1 ));

    }
}