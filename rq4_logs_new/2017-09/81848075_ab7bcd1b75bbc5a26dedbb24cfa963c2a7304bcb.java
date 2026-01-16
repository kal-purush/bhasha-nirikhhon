package controllers;

import java.util.List;

import models.DnevnoStanjeRacuna;
import models.Klijent;
import models.Racun;
import play.data.validation.Required;
import play.mvc.Controller;
import play.mvc.With;

/**
 * Created by Djordje on 3/26/2017.
 */
@With(Secure.class)
public class RacuniAdmin extends Controller {

    public static void show(String mode){
        List<Racun> racuni = Racun.findAll();
        render(racuni, mode);
    }

    public static void delete(long id){
        Racun racun = Racun.findById(id);
        racun.delete();
        show("");
    }

    public static void add(){
        System.out.println("bla bla bla");
        show("add");
    }

    public static void edit(){
        show("edit");
    }
}