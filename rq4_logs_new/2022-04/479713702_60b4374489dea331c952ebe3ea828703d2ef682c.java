/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package converter;
import dao.WriterDao;
import entity.Writer;
import jakarta.faces.component.UIComponent;
import jakarta.faces.context.FacesContext;
import jakarta.faces.convert.Converter;
import jakarta.faces.convert.FacesConverter;

import jakarta.faces.convert.FacesConverter;
/**
 *
 * @author mabea
 */
@FacesConverter("writerConverter")
public class WriterConverter implements Converter{

    private WriterDao writerDao;

    public WriterDao getWriterDao() {
        if(this.writerDao == null){
            writerDao = new WriterDao();
        }
        return writerDao;
    }

    public void setWriterDao(WriterDao writerDao) {
        this.writerDao = writerDao;
    }
    
    
    
    public Object getAsObject(FacesContext fc, UIComponent uic, String string) {
           int id= Integer.valueOf(string);
           Writer c = this.getWriterDao().findById(id);
           return c;
    }

   
    public String getAsString(FacesContext fc, UIComponent uic, Object t) {
           Writer c = (Writer) t;
           return String.valueOf(c.getId());

    }
    
}