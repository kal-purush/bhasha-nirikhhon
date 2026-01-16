package mg.itu.prom16.modelView;

import java.util.HashMap;

public class ModelView {

    private String nameView;
    private HashMap<String,Object> listValue;

    public ModelView(String nameView){
        this.nameView = nameView;
        this.listValue = new HashMap<>();
    }

    public String getNameView(){
        return "/" + nameView;
    }

    public void setName(String name){
        this.nameView = name;
    }

    public HashMap<String, Object> getListValue(){
        return listValue;
    }

    public void setListValue(HashMap<String, Object> list) {
        this.listValue = list;
    }

    public void addObject(String key, Object value){
        this.getListValue().put(key, value);
    }
}