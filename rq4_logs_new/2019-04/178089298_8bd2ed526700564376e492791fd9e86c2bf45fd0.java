package com.fiuba.tdpii.correapp.models.web.driver;
import com.fiuba.tdpii.correapp.models.web.Destination;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class DriverResponse {

    @SerializedName("dni")
    @Expose
    private String dni;
    @SerializedName("name")
    @Expose
    private String name;
    @SerializedName("lastname")
    @Expose
    private String lastname;
    @SerializedName("email")
    @Expose
    private String email;
    @SerializedName("telephone")
    @Expose
    private String telephone;
    @SerializedName("celphone")
    @Expose
    private String celphone;
    @SerializedName("address")
    @Expose
    private String address;
    @SerializedName("brand")
    @Expose
    private String brand;
    @SerializedName("model")
    @Expose
    private String model;
    @SerializedName("licensenumber")
    @Expose
    private Long licensenumber;
    @SerializedName("insurancepolicynumber")
    @Expose
    private String insurancepolicynumber;
    @SerializedName("startworktime")
    @Expose
    private String startworktime;
    @SerializedName("endworktime")
    @Expose
    private String endworktime;
    @SerializedName("carlicenseplate")
    @Expose
    private String carlicenseplate;
    @SerializedName("carcolour")
    @Expose
    private String carcolour;

    @SerializedName("currentposition")
    @Expose
    private Destination currentposition;

    @SerializedName("id")
    @Expose
    private Long id;

    @SerializedName("rating")
    @Expose
    private Double rating;

    @SerializedName("photo_url")
    @Expose
    private String photoUrl;

    public String getPhotoUrl() {
        return photoUrl;
    }

    public void setPhotoUrl(String photoUrl) {
        this.photoUrl = photoUrl;
    }

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDni() {
        return dni;
    }

    public void setDni(String dni) {
        this.dni = dni;
    }


    public Destination getCurrentposition() {
        return currentposition;
    }

    public void setCurrentposition(Destination currentposition) {
        this.currentposition = currentposition;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getCelphone() {
        return celphone;
    }

    public void setCelphone(String celphone) {
        this.celphone = celphone;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public Long getLicensenumber() {
        return licensenumber;
    }

    public void setLicensenumber(Long licensenumber) {
        this.licensenumber = licensenumber;
    }

    public String getInsurancepolicynumber() {
        return insurancepolicynumber;
    }

    public void setInsurancepolicynumber(String insurancepolicynumber) {
        this.insurancepolicynumber = insurancepolicynumber;
    }

    public String getStartworktime() {
        return startworktime;
    }

    public void setStartworktime(String startworktime) {
        this.startworktime = startworktime;
    }

    public String getEndworktime() {
        return endworktime;
    }

    public void setEndworktime(String endworktime) {
        this.endworktime = endworktime;
    }

    public String getCarlicenseplate() {
        return carlicenseplate;
    }

    public void setCarlicenseplate(String carlicenseplate) {
        this.carlicenseplate = carlicenseplate;
    }

    public String getCarcolour() {
        return carcolour;
    }

    public void setCarcolour(String carcolour) {
        this.carcolour = carcolour;
    }
}