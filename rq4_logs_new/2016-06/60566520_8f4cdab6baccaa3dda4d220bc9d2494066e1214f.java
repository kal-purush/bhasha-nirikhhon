package org.woehlke.javaee7.petclinic.web;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;
import java.io.Serializable;
import java.util.logging.Logger;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.logging.Level;
import org.codehaus.jackson.map.ObjectMapper;
import org.woehlke.javaee7.petclinic.model.Forecast;
import org.woehlke.javaee7.petclinic.model.ForecastList;
import org.woehlke.javaee7.petclinic.model.Weather;

@ManagedBean
@SessionScoped
public class ForecastController implements Serializable {

    private static final Logger log = Logger.getLogger(ForecastController.class.getName());

    private String searchterm;

    private Forecast fc;
    
    private ArrayList<ForecastList> fl;

    public ArrayList<ForecastList> getFl() {
        return fl;
    }

    public String getSearchterm() {
        return searchterm;
    }

    public void setSearchterm(String searchterm) {
        this.searchterm = searchterm;
    }

    public String search() {

        ObjectMapper mapper = new ObjectMapper();
        ForecastList f;
        fl = new ArrayList<>();
        try {
            fc = mapper.readValue(new URL("http://api.openweathermap.org/data/2.5/forecast/daily?q=" + this.searchterm + "&lang=pt&cnt=7&units=metric&mode=json&APPID=9c6cc69cad227d2f2b7c54783bdb1cc4"), Forecast.class);
            log.log(Level.INFO, "{0},{1}",new Object[]{fc.getCity().getName(),fc.getCity().getCountry()});

            String date;
            for (org.woehlke.javaee7.petclinic.model.List ls : fc.getList()) {
                date = formatDate(ls.getDt());
                Weather wt = ls.getWeather().get(0);
                
                f = new ForecastList();
                f.setCity(fc.getCity().getName());
                f.setCountry(fc.getCity().getCountry());
                f.setDate(date);
                f.setTemp(ls.getTemp().getDay().intValue());
                f.setTemp_max(ls.getTemp().getMax().intValue());
                f.setTemp_min(ls.getTemp().getMin().intValue());
                f.setTemp_manha(ls.getTemp().getMorn().intValue());
                f.setTemp_tarde(ls.getTemp().getEve().intValue());
                f.setTemp_noite(ls.getTemp().getNight().intValue());
                f.setUmidade(ls.getHumidity());
                f.setPressao(ls.getPressure());
                f.setPrevisao(wt.getDescription());
                f.setIcone(wt.getIcon());
                fl.add(f);
                
                log.log(Level.WARNING, "Data: {0}", date);
                log.log(Level.WARNING, "Temperatura do Dia: {0}°C", ls.getTemp().getDay().intValue());
                log.log(Level.WARNING, "Temperatura Máxima: {0}°C", ls.getTemp().getMax().intValue());
                log.log(Level.WARNING, "Temperatura Mínima: {0}°C", ls.getTemp().getMin().intValue());
                log.log(Level.WARNING, "Temperatura Manhã: {0}°C", ls.getTemp().getMorn().intValue());
                log.log(Level.WARNING, "Temperatura Tarde: {0}°C", ls.getTemp().getEve().intValue());
                log.log(Level.WARNING, "Temperatura Noite: {0}°C", ls.getTemp().getNight().intValue());
                log.log(Level.WARNING, "Umidade: {0}%", ls.getHumidity());
                log.log(Level.WARNING, "Previs\u00e3o: {0}", wt.getDescription());
                log.log(Level.WARNING, "Press\u00e3o: {0}", ls.getPressure());
                log.log(Level.WARNING, "Icone: {0}", wt.getIcon());

                log.warning("\n");
            };

        } catch (IOException ex) {
            Logger.getLogger(ForecastController.class.getName()).log(Level.SEVERE, null, ex);
        }

        return "forecast.jsf";
    }
    
    public String formatDate(long dt){
        dt = dt * 1000L;
        SimpleDateFormat sdf = new SimpleDateFormat("MMMM d, yyyy 'at' h:mm a");
        String date = sdf.format(dt);
        return date;
    }
}