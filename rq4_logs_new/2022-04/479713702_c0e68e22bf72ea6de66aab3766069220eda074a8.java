
package entity;

import java.util.Date;


public class Blog {
    private int id;
    private String title;
    private String content;
    private String image;
    private Date create_date;
    private int category_id;
    private Boolean status;
    private int writer_id;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public Date getCreate_date() {
        return create_date;
    }

    public void setCreate_date(Date create_date) {
        this.create_date = create_date;
    }

    public int getCategory_id() {
        return category_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public Boolean getStatus() {
        return status;
    }

    public void setStatus(Boolean status) {
        this.status = status;
    }

    public int getWriter_id() {
        return writer_id;
    }

    public void setWriter_id(int writer_id) {
        this.writer_id = writer_id;
    }

    public Blog(int id, String title, String content, String image, Date create_date, int category_id, Boolean status, int writer_id) {
        this.id = id;
        this.title = title;
        this.content = content;
        this.image = image;
        this.create_date = create_date;
        this.category_id = category_id;
        this.status = status;
        this.writer_id = writer_id;
    }
}