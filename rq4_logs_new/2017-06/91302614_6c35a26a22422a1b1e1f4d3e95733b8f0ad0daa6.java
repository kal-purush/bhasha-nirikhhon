package com.quick.location.model;
import java.util.List;

/**
 * Created by Rosendo on 17/06/2017.
 * Esta clase define la etiqueta y el contenido que difiere al conjunto
 * de informacion que puede ser mejorada por el usuario a traves de la
 * aplicacion web
 */

public class ImprovementInformation {
    private String informationTag;
    private String informationContent;
    private Object isSchedule;
    
    public Object getIsSchedule() {
		return isSchedule;
	}

	public void setIsSchedule(Object isSchedule) {
		this.isSchedule = isSchedule;
	}

	public void setSchedule(boolean isSchedule) {
		this.isSchedule = isSchedule;
	}

	private List<Schedule> schedules;
    private String author;

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public List<Schedule> getSchedules() {
        return schedules;
    }

    public void setSchedules(List<Schedule> schedules) {
        this.schedules = schedules;
    }

    public String getInformationTag() {
        return informationTag;
    }

    public void setInformationTag(String informationTag) {
        this.informationTag = informationTag;
    }

    public String getInformationContent() {
        return informationContent;
    }

    public void setInformationContent(String informationContent) {
        this.informationContent = informationContent;
    }
    
}