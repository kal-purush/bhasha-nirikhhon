package models;

import java.io.Serializable;
import java.util.List;

//import javax.faces.bean.ManagedBean;
import javax.annotation.ManagedBean;
import javax.enterprise.context.SessionScoped;
import javax.inject.Named;

@ManagedBean
@SessionScoped
@Named
public class FilmHits implements Serializable {

	public FilmHits() {
		super();
	}
	
    private List<Film> hits;

    public List<Film> getHits() {
        return hits;
    }

    public void setHits(List<Film> films) {
        this.hits = films;
    }
}