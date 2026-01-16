package red.htt.excels.domain;


import java.util.List;

/**
 * @author yui
 */
public class SheetData<T> {

    private List<T> data;
    private Class<T> clazz;

    private String sheetName;

    public SheetData() {
    }

    public SheetData(List<T> data, Class<T> clazz, String sheetName) {
        this.data = data;
        this.clazz = clazz;
        this.sheetName = sheetName;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public Class<T> getClazz() {
        return clazz;
    }

    public void setClazz(Class<T> clazz) {
        this.clazz = clazz;
    }

    public String getSheetName() {
        return sheetName;
    }

    public void setSheetName(String sheetName) {
        this.sheetName = sheetName;
    }
}