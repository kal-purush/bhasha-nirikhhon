package com.mauliamahardika.cobalogindenganxampp.fiturrecyclemasatanam;

public class Produk {
    private int seri;
    private int id;
    private String namapenanam;
    private String jenistanaman;
    private String jumlahtanaman;
    private String tglsemai;
    private String prediksiparalon;
    private String prediksipanen;
    private String estimasipenjualan;

    public Produk(int seri,int id, String namapenanam, String jenistanaman,String jumlahtanaman, String tglsemai,String prediksiparalon,String prediksipanen, String estimasipenjualan) {
        this.seri=seri;
        this.id=id;
        this.namapenanam=namapenanam;
        this.jenistanaman=jenistanaman;
        this.jumlahtanaman=jumlahtanaman;
        this.tglsemai=tglsemai;
        this.prediksiparalon=prediksiparalon;
        this.prediksipanen=prediksipanen;
        this.estimasipenjualan=estimasipenjualan;

        /*this.id = id;
        this.title = title;
        this.shortdesc = shortdesc;
        this.rating = rating;
        this.price = price;
        this.image = image;*/
    }

    public int getSeri(){
        return seri;
    }

    public int getId() {
        return id;
    }

    public String getNamapenanam(){
        return namapenanam;
    }
    public String getJenistanaman(){
        return jenistanaman;
    }
    public String getJumlahtanaman(){
        return jumlahtanaman;
    }
    public String getTglsemai(){
        return tglsemai;
    }
    public String getPrediksiparalon (){
        return prediksiparalon;
    }
    public String getPrediksipanen(){
        return prediksipanen;
    }
    public String getEstimasipenjualan(){
        return estimasipenjualan;
    }
   /* public int getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getShortdesc() {
        return shortdesc;
    }

    public double getRating() {
        return rating;
    }

    public double getPrice() {
        return price;
    }

    public String getImage() {
        return image;
    }*/
}