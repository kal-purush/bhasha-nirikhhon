package md.tekwill.topic5;

public class Cerc extends FiguraGeometrica {

    private double razaCerului;

    public void setRazaCerului(double razaCerului) {
        if (razaCerului<0){
            System.out.println("Raza cercului are valoare negativa.");
        }else {
            System.out.println("Raza cercului are valoare pozitiva.");
            this.razaCerului = razaCerului;
        }
    }

    public double getRazaCerului() {
        return razaCerului;
    }

    @Override
    public double returneazaAria() {
        this.ariaFigurii = 3.14*razaCerului*razaCerului;
        return this.ariaFigurii;
    }

    @Override
    public double returneazaPerimetrul() {
        this.perimetrulFigurii = 2*3.14*razaCerului;
        return this.perimetrulFigurii;
    }
}
