package com.example.junior.volleyapp.model;

import android.content.Context;
import android.util.Log;

import com.android.volley.Request;
import com.android.volley.RequestQueue;
import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.Volley;
import com.example.junior.volleyapp.conexao.CustomJsonArraytRequest;

import org.json.JSONArray;
import org.json.JSONException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by junior on 09/05/15.
 */
public class NovoPedidoModel {
    private String url = "";
    private Map<String,String> params;
    private String desc;
    private String produto;
    private String nnotas;
    private String data;
    private String iduser;
    private RequestQueue rq;

    public NovoPedidoModel(){}
    public NovoPedidoModel(Context context, String desc, String produto, String nnotas, String data, String iduser){
        this.setDesc(desc);
        this.setProduto(produto);
        this.setNnotas(nnotas);
        this.setData(data);
        this.setIduser(iduser);

        rq = Volley.newRequestQueue(context);
    }
    private String insertUser(){

        final String[] res = {""};

        params = new HashMap<String, String>();
        params.put("desc",getDesc());
        params.put("produtos",getProduto());
        params.put("nnotas",getNnotas());
        params.put("data",getData());
        params.put("iduser",getIduser());

        CustomJsonArraytRequest jar = new CustomJsonArraytRequest(Request.Method.POST, url, params,
                new Response.Listener<JSONArray>() {
            @Override
            public void onResponse(JSONArray response) {
                try {
                    res[0] = String.valueOf(response.get(0));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }, new Response.ErrorListener() {
            @Override
            public void onErrorResponse(VolleyError volleyError) {
                Log.i("INSERT ERROR", volleyError.toString());
            }
        });

        jar.setTag("novo");
        rq.add(jar);

        return res[0];
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