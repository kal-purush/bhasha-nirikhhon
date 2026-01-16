import dao.LocomotiveDAO;
import model.Locomotive;

public class Run {
    public static void main(String[] args) {
        Locomotive locomotive1 = new Locomotive(11L, "Loco", 100, 1000, 1990, Locomotive.FuelType.DIESEL);
        Locomotive locomotive2 = new Locomotive(15L, "Loco", 100, 1000, 1990, Locomotive.FuelType.DIESEL);
        LocomotiveDAO locomotiveDAO = new LocomotiveDAO();
        //locomotiveDAO.saveEntity(locomotive1);
        //locomotiveDAO.saveEntity(locomotive2);
        //locomotiveDAO.removeEntity(locomotive1);
        locomotiveDAO.saveEntity(locomotive2);

    }
}