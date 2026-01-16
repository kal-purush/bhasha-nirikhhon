package com.example.junior.volleyapp;

import android.text.TextUtils;

import com.example.junior.volleyapp.conexao.CustomJsonArraytRequest;
import com.example.junior.volleyapp.conexao.CustomJsonObjectRequest;

import org.w3c.dom.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by junior on 09/05/15.
 */
public class NovoPedido {
    private String url = "";
    private Map<String,String> params;
    private String desc;
    private String produto;
    private String nnotas;
    private String data;
    private String iduser;

    public NovoPedido(){}
    public NovoPedido(String desc, String produto, String nnotas, String data, String iduser){
        this.setDesc(desc);
        this.setProduto(produto);
        this.setNnotas(nnotas);
        this.setData(data);
        this.setIduser(iduser);
    }
    private String insertUser(){


        params = new HashMap<String, String>();
        params.put("desc",getDesc());
        params.put("produtos",getProduto());
        params.put("nnotas",getNnotas());
        params.put("data",getData());
        params.put("iduser",getIduser());

        CustomJsonObjectRequest jor = new CustomJsonObjectRequest();

        return "inserted";
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getProduto() {
        return produto;
    }

    public void setProduto(String produto) {
        this.produto = produto;
    }

    public String getNnotas() {
        return nnotas;
    }

    public void setNnotas(String nnotas) {
        this.nnotas = nnotas;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getIduser() {
        return iduser;
    }

    public void setIduser(String iduser) {
        this.iduser = iduser;
    }
}