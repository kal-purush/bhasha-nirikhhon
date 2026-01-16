package entities;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import entities.Product;


public class Order {
    private Long Id;
    private Double latitude;
    private Double longtude;
    private Instant moment;
    private OrderStatus status;

    private List<Product> products = new ArrayList<>();    




	/**
     * @return Long return the Id
     */
    public Long getId() {
        return Id;
    }

    /**
     * @param Id the Id to set
     */
    public void setId(Long Id) {
        this.Id = Id;
    }

    /**
     * @return Double return the latitude
     */
    public Double getLatitude() {
        return latitude;
    }

    /**
     * @param string the latitude to set
     */
    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    /**
     * @return Double return the longtude
     */
    public Double getLongtude() {
        return longtude;
    }

    /**
     * @param string the longtude to set
     */
    public void setLongtude(Double longtude) {
        this.longtude = longtude;
    }

    /**
     * @return Instant return the moment
     */
    public Instant getMoment() {
        return moment;
    }

    /**
     * @param moment the moment to set
     */
    public void setMoment(Instant moment) {
        this.moment = moment;
    }

    /**
     * @return OrderStatus return the status
     */
    public OrderStatus getStatus() {
        return status;
    }

    /**
     * @param d the status to set
     */
    public void setStatus(OrderStatus status) {
        this.status = status;
    }
     

    /**
     * @return List<Product> return the products
     */
    public List<Product> getProducts() {
        return products;
    }

    /**
     * @param products the products to set
     */
    public void setProducts(List<Product> products) {
        this.products = products;
    }


	@Override
	public String toString() {
		return "Order [Id=" + Id + ", latitude=" + latitude + ", longtude=" + longtude + ", moment=" + moment
				+ ", status=" + status + "]";
	}




    
}