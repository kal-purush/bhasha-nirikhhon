package org.kannegiesser.simpletodo;

import com.activeandroid.Model;
import com.activeandroid.annotation.Column;
import com.activeandroid.annotation.Table;

@Table(name = "items")
public class Item extends Model {
    @Column(name = "text", notNull = true)
    public String text;

    public Item() {
        super();
    }

    public Item(String text) {
        super();
        this.text = text;
    }

    public String toString() {
        return text;
    }
}