package md.tekwill.topic2;

public class Persoana {
   public Persoana(String prenumeAtribuit, String numeAtribuit, int varstaAtribuita) {
    prenume = prenumeAtribuit;
    nume = numeAtribuit;
    varsta = varstaAtribuita;
   }
//
    String prenume;
    String nume;
    int varsta;
    private String gen;
    private double greutate;

void afiseazaPersoana () {
    System.out.println(prenume + " " + nume + " " + "are varsta " + varsta + " ani" );
}
public void seteazaGen (String genAtribuit) {
    this.gen = genAtribuit;
    System.out.println("Genul determinat este " + gen);
}
public String returneazaGen () {
    return this.gen;
}
public void seteazaGreutate (double greutateAtribuita) {
    this.greutate = greutateAtribuita;
    System.out.println("Greutatea determinata este " + greutate);
}

public double returneazaGreutate () {
    return this.greutate;
}

}