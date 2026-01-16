import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Intrebari {
    private int id;
    private String text_intrebare;
    private String raspuns;
    private int punctaj;

    public Intrebari() {
    }

    public Intrebari(int id, String text_intrebare, String raspuns, int punctaj) {
        this.id = id;
        this.text_intrebare = text_intrebare;
        this.raspuns = raspuns;
        this.punctaj = punctaj;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getText_intrebare() {
        return text_intrebare;
    }

    public void setText_intrebare(String text_intrebare) {
        this.text_intrebare = text_intrebare;
    }

    public String getRaspuns() {
        return raspuns;
    }

    public void setRaspuns(String raspuns) {
        this.raspuns = raspuns;
    }

    public int getPunctaj() {
        return punctaj;
    }

    public void setPunctaj(int punctaj) {
        this.punctaj = punctaj;
    }

    @Override
    public String toString() {
        return "Intrebari{" +
                "id = " + id +
                ", text_intrebare = '" + text_intrebare + '\'' +
                ", raspuns = '" + raspuns + '\'' +
                ", punctaj = " + punctaj +
                '}'+"\n";
    }

    public List<Intrebari> citireTxt(String numeFisier){
        List<Intrebari> listaIntrebari=new ArrayList<>();
        File file=new File(numeFisier);
        try {
            Scanner scanner =new Scanner(file);
            while(scanner.hasNextLine()){
                String linie=scanner.nextLine();
                var bucati=linie.split(",");

                int id=Integer.parseInt(bucati[0]);
                String text_intrebare=bucati[1];
                String raspuns = bucati[2];
                int punctaj =Integer.parseInt(bucati[3]);

                Intrebari intrebari=new Intrebari(id, text_intrebare,raspuns,punctaj);
                listaIntrebari.add(intrebari);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return listaIntrebari;
    }

}//end class