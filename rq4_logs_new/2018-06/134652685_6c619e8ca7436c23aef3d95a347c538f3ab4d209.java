package ca.huynhat.itsasteal.models;

import com.google.android.gms.maps.model.LatLng;

public class Deal {
    private String deal_id;
    private String item_name;
    private String item_desc;
    private int quantity;
    private String store_name;
    private LatLng location_coordinate;
    private String time_stamp;
    private String deal_img_url;

    Deal(){

    }

    public Deal(String deal_id, String item_name, String item_desc, int quantity,
                String store_name, LatLng location_coordinate, String time_stamp, String deal_img_url) {
        this.deal_id = deal_id;
        this.item_name = item_name;
        this.item_desc = item_desc;
        this.quantity = quantity;
        this.store_name = store_name;
        this.location_coordinate = location_coordinate;
        this.time_stamp = time_stamp;
        this.deal_img_url = deal_img_url;
    }

    public String getDeal_id() {
        return deal_id;
    }

    public void setDeal_id(String deal_id) {
        this.deal_id = deal_id;
    }

    public String getItem_name() {
        return item_name;
    }

    public void setItem_name(String item_name) {
        this.item_name = item_name;
    }

    public String getItem_desc() {
        return item_desc;
    }

    public void setItem_desc(String item_desc) {
        this.item_desc = item_desc;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getStore_name() {
        return store_name;
    }

    public void setStore_name(String store_name) {
        this.store_name = store_name;
    }

    public LatLng getLocation_coordinate() {
        return location_coordinate;
    }

    public void setLocation_coordinate(LatLng location_coordinate) {
        this.location_coordinate = location_coordinate;
    }

    public String getTime_stamp() {
        return time_stamp;
    }

    public void setTime_stamp(String time_stamp) {
        this.time_stamp = time_stamp;
    }

    public String getDeal_img_url() {
        return deal_img_url;
    }

    public void setDeal_img_url(String deal_img_url) {
        this.deal_img_url = deal_img_url;
    }

    @Override
    public String toString() {
        return "Deal{" +
                "deal_id='" + deal_id + '\'' +
                ", item_name='" + item_name + '\'' +
                ", item_desc='" + item_desc + '\'' +
                ", quantity=" + quantity +
                ", store_name='" + store_name + '\'' +
                ", location_coordinate=" + location_coordinate +
                ", time_stamp='" + time_stamp + '\'' +
                ", deal_img_url='" + deal_img_url + '\'' +
                '}';
    }
}
