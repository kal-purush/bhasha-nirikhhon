package hcmute.edu.vn.myfoody;

public class Store {
    private Integer StoreId;
    private String StoreName;
    private String Address;
    private String OpenTime;
    private String CloseTime;
    private Integer CategoryId;
    private byte[] CoverPhoto;
    private Float StoreRateStar;
    private String Email;

    public Store(Integer storeId, String storeName, String address, String openTime,
                 String closeTime, Integer categoryId, byte[] coverPhoto, Float storeRateStar, String email) {
        StoreId = storeId;
        StoreName = storeName;
        Address = address;
        OpenTime = openTime;
        CloseTime = closeTime;
        CategoryId = categoryId;
        CoverPhoto = coverPhoto;
        StoreRateStar = storeRateStar;
        Email = email;
    }

    public Integer getStoreId() {
        return StoreId;
    }

    public void setStoreId(Integer storeId) {
        StoreId = storeId;
    }

    public String getStoreName() {
        return StoreName;
    }

    public void setStoreName(String storeName) {
        StoreName = storeName;
    }

    public String getAddress() {
        return Address;
    }

    public void setAddress(String address) {
        Address = address;
    }

    public String getOpenTime() {
        return OpenTime;
    }

    public void setOpenTime(String openTime) {
        OpenTime = openTime;
    }

    public String getCloseTime() {
        return CloseTime;
    }

    public void setCloseTime(String closeTime) {
        CloseTime = closeTime;
    }

    public Integer getCategoryId() {
        return CategoryId;
    }

    public void setCategoryId(Integer categoryId) {
        CategoryId = categoryId;
    }

    public byte[] getCoverPhoto() {
        return CoverPhoto;
    }

    public void setCoverPhoto(byte[] coverPhoto) {
        CoverPhoto = coverPhoto;
    }

    public Float getStoreRateStar() {
        return StoreRateStar;
    }

    public void setStoreRateStar(Float storeRateStar) {
        StoreRateStar = storeRateStar;
    }

    public String getEmail() {
        return Email;
    }

    public void setEmail(String email) {
        Email = email;
    }
}