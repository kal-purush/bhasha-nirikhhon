package helloPackage.dataObject;

import com.googlecode.objectify.Key;
import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import com.googlecode.objectify.annotation.Index;
import com.googlecode.objectify.annotation.Parent;

/**
 * Created by Yifang Cao on 2/11/2017.
 */
@Entity
public class TodoItem {
    @Parent @Index private Key<TodoList> theList;
    //have to be capitalized L-ong, not long, its value will be auto-generated
    //Primary Key
    @Id public Long id;
    // five attributes
    @Index private String category;
    @Index private String description;
    @Index private String startDate;
    @Index private String endDate;
    @Index private String completed;


    public TodoItem(){
        this.category = "";
        this.description = "";
        this.startDate = "";
        this.endDate = "";
        this.completed = "";
    }
    public TodoItem(String category, String description,String startDate,
                    String endDate, String completed, TodoList theParentList){
        this();
        theList = Key.create(TodoList.class,theParentList.id);
        if(category!=null){
            this.category = category;
        }
        if(description!=null){
            this.description = description;
        }
        if(startDate!=null){
            this.startDate = startDate;
        }
        if(this.endDate!=null){
            this.endDate = endDate;
        }
        if(completed!=null){
            this.completed = completed;
        }
    }


    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getCompleted() {
        return completed;
    }

    public void setCompleted(String completed) {
        this.completed = completed;
    }

    public Key<TodoList> getTheParentList() {
        return theList;
    }

    public void setTheParentList(Key<TodoList> theList) {
        this.theList = theList;
    }
}