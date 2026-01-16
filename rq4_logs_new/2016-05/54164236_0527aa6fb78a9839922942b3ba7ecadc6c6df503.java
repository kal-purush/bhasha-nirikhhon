package ohtu.kivipaperisakset;

/**
 *
 * @author nikok
 */
public class Ihmispelaaja extends Pelaaja {
    
    @Override
    public String getSiirto(String vastustajanEdellinenSiirto) {
        return scanner.nextLine();
    }
    
}