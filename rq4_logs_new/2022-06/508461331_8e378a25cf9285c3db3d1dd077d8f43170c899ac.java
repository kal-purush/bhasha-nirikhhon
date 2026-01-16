package tercerParcial.E2;

public class Client {
    public static void main(String[] args) throws InterruptedException {
        ICuadratrack cuadratrackG = new Gasolina();
        cuadratrackG.cargarCombustible();


        ICuadratrack cuadratrackGE = new GasolinaEspecial();
        cuadratrackGE.estadoCombustible();

        ICuadratrack cuadratrackD = new Diesel();
        cuadratrackD.cargarCombustible();

        ICuadratrack cuadratrackE = new CuadratrackElectrica(new CuadraTrack());
        cuadratrackE.estadoCombustible();
    }
}