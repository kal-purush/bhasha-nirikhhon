package org.example.model.domain;

import org.example.model.domain.entity.Product;
import org.example.model.repository.DBInteraction;
import org.example.model.repository.DatabaseConnection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlowerShop {
    private String name;
    private List<Product> stockList;
    private List<Invoice> invoiceLog;
    private double totalEarnings;
    private double stockTotalValue;
    private Map<String,Integer> productMap;

    public FlowerShop(String name) {
        this.name = name;
        this.stockList = new ArrayList<>();
        this.invoiceLog = new ArrayList<>();
        this.totalEarnings = 0.0d;
        this.stockTotalValue = 0.0d;
        this.productMap = new HashMap<>();
        createMap();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Product> getStockList() {
        return stockList;
    }

    public void setStockList(List<Product> stockList) {
        this.stockList = stockList;
    }

    public List<Invoice> getInvoiceLog() {
        return invoiceLog;
    }

    public void setInvoiceLog(List<Invoice> invoiceLog) {
        this.invoiceLog = invoiceLog;
    }

    public double getTotalEarnings() {
        return totalEarnings;
    }

    public void setTotalEarnings(double totalEarnings) {
        this.totalEarnings = totalEarnings;
    }

    public double getStockTotalValue() {
        return stockTotalValue;
    }

    public void setStockTotalValue(double stockTotalValue) {
        this.stockTotalValue = stockTotalValue;
    }

    public Map<String, Integer> getProductMap() {
        return productMap;
    }

    public void setProductMap(Map<String, Integer> productMap) {
        this.productMap = productMap;
    }

    public void createMap(){
        productMap.put("Tree",0);
        productMap.put("Flower",0);
        productMap.put("Decoration",0);
    }

    public void addStock(Product p){
        stockList.add(p);
        stockTotalValue += p.getPrice();
        productMap.put(p.getType(), (int)productMap.get(p.getType()) + 1);
    }

    public void removeStock(Product p){
        stockList.remove(p);
        stockTotalValue -= p.getPrice();
        productMap.put(p.getType(), (int)productMap.get(p.getType()) - 1);
    }

    public void showStock(){
        stockList.forEach(System.out::println);
        System.out.print("\n");
    }

    public void addInvoice(Invoice i ){
        invoiceLog.add(i);
        totalEarnings += i.getTotalSale();
    }

    public void removeInvoice(Invoice i){
        if(invoiceLog.contains(i)){
            invoiceLog.remove(i);
            totalEarnings -= i.getTotalSale();
        }
    }

    public void showInvoiceList() {
        invoiceLog.forEach(System.out::println);
    }

}