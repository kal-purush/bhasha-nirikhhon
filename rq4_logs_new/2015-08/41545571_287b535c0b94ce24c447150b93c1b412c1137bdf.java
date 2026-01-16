/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.setha;

import com.setha.domain.Barang;
import com.setha.domain.User;
import com.setha.domain.Penjualan;
import com.setha.service.Implement;
import com.setha.service.Interface;

/**
 *
 * @author aviani
 */
public class Main {

    public static void main(String[] args) {
        Implement service = new Interface();
        service.save(new Barang());
        service.update(new Barang());
        service.delete(new Barang());
        service.selectBarang();
        
        System.out.println("******************************");
        service.save(new User());
        service.update(new User());
        service.delete(new User());
        service.selectUser();
        
        System.out.println("******************************");
        service.save(new Penjualan());
        service.update(new Penjualan());
        service.delete(new Penjualan());
        service.selectPenjualan();
}}