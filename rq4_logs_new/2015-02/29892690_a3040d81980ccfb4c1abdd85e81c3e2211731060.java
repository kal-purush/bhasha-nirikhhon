package com.quiztads.ufpr.br.quiztads;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;


public class DetectaConexao {
    private Context _context;

    public DetectaConexao(Context context){
        this._context = context;
    }

    public boolean existeConexao(){
        ConnectivityManager connectivity = (ConnectivityManager)
                _context.getSystemService(Context.CONNECTIVITY_SERVICE);
        if (connectivity != null)
        {
            NetworkInfo netInfo = connectivity.getActiveNetworkInfo();

            // Se não existe nenhum tipo de conexão retorna false
            if (netInfo == null) {
                return false;
            }

            int netType = netInfo.getType();
            // Verifica se a conexão é do tipo WiFi ou Mobile e
            // retorna true se estiver conectado ou false em
            // caso contrário
            if (netType == ConnectivityManager.TYPE_WIFI ||
                    netType == ConnectivityManager.TYPE_MOBILE) {
                return netInfo.isConnected();

            } else {
                return false;
            }

        }else{
            return false;
        }
    }
}