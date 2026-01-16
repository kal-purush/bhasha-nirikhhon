package JWS;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
//POJO plain old java object : un simple class java il y a pas des exigences
@WebService(name = "BanqueWS")
public class BanqueService {
    @WebMethod(operationName = "ConversionEuroToDh")
    public double conversion( @WebParam(name = "montant") double mnt){
        return mnt*11;
    }
    public Compte getCompte(@WebParam(name = "code") int code){
        return new Compte(code,Math.random()*1000,new Date());
    }
    @WebMethod
    public List<Compte> getComptes(){
        AtomicInteger count = new AtomicInteger(1);
        List<Compte> compteList=new ArrayList<>(4);
        for (int i=0;i<3;i++) {
            compteList.add(new Compte(count.getAndIncrement(),Math.random()*10000,new Date()));
        }
        return compteList;
    }
}