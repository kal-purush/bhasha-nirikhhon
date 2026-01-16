public class ProfileTest {
    public static void main( String[] args ) {
        Scanner sc = new Scanner(System.in);
        System.out.println( "Enter user hash:" );
        String hash = sc.nextLine();
        Profile me = new Profile( new HttpRequest( hash ) );
        System.out.println( me.toString() );
    }
}