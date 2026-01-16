package com.hotsauce.meem.db;

import android.graphics.Point;
import android.graphics.Rect;
import android.os.Environment;

import androidx.annotation.NonNull;
import androidx.core.util.Pair;
import androidx.room.ColumnInfo;
import androidx.room.Entity;
import androidx.room.PrimaryKey;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Entity(tableName = "template_table")
public class MemeTemplate implements Serializable {

    @PrimaryKey
    @NonNull
    @ColumnInfo(name = "id")
    private String id;

    @NonNull
    @ColumnInfo(name = "coord_string")
    private String coordString;

    public MemeTemplate(@NonNull String id, @NonNull String coordString) {
        this.id = id;
        this.coordString = coordString;
    }

    public MemeTemplate(@NonNull String id, List<Rect> rectangles) {
        this.id = id;
        StringBuilder temp = new StringBuilder();
        for (int i = 0; i < rectangles.size(); i++) {
            Rect r = rectangles.get(i);
            // left top right bottom
            temp.append(String.format("%d,%d,%d,%d", r.left, r.top, r.right, r.bottom));
        }
        this.coordString = temp.toString();
    }

    @NonNull
    public String getId() {
        return id;
    }

    @NonNull
    public String getCoordString() {
        return coordString;
    }

    public List<Rect> getRectangles() {
        ArrayList<Rect> rectList = new ArrayList<>();
        String[] rectStrings = coordString.split("#");
        for (String rectString : rectStrings) {
            String[] r = rectString.split(",");
            rectList.add(new Rect(
                    Integer.parseInt(r[0]),
                    Integer.parseInt(r[1]),
                    Integer.parseInt(r[2]),
                    Integer.parseInt(r[3])
            ));
        }
        return rectList;
    }

    public String getFilepath() {
        return Environment.getExternalStorageDirectory().getAbsolutePath() + File.separator + id + ".png";
    }
}