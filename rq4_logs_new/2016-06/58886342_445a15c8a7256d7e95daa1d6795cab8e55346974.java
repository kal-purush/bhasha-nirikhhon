package entity;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by Alex on 29.05.2016.
 */

@XmlRootElement(name = "annotation")
@XmlAccessorType(XmlAccessType.FIELD)
public class Annotation {

    @XmlAttribute(name = "page-number")
    private int pageNumber;

    @XmlAttribute(name = "figure-number")
    private int figureNumber;

    @XmlAttribute(name = "table-number")
    private int tableNumber;

    public int getPageNumber() {
        return pageNumber;
    }

    public int getFigureNumber() {
        return figureNumber;
    }

    public int getTableNumber() {
        return tableNumber;
    }

    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }

    public void setFigureNumber(int fihureNumber) {
        this.figureNumber = fihureNumber;
    }

    public void setTableNumber(int tableNumber) {
        this.tableNumber = tableNumber;
    }

    @Override
    public String toString() {
        return "Annotation{" +
                "pageNumber=" + pageNumber +
                ", figureNumber=" + figureNumber +
                ", tableNumber=" + tableNumber +
                '}';
    }
}