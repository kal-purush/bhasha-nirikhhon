package anbinc.flickr;

/**
 * Created by Alex on 16.02.2017.
 */
public class Picture {
    private String id;
    private String name;
    private int usageCount;

    public Picture(String id) {
        setId(id);
    }

    public void use()   {
        usageCount++;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
        this.name = FlickrApi.getPhotoName(id);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getUsageCount() {
        return usageCount;
    }

    public void setUsageCount(int usageCount) {
        this.usageCount = usageCount;
    }
}