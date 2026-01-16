package projetoSaude.mobile.Sensoriando;

import android.app.AlertDialog;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Bundle;
import android.os.PowerManager;
import android.support.v4.widget.DrawerLayout;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ListView;
import android.widget.RelativeLayout;
import android.widget.TextView;
import android.widget.Toast;

import projetoSaude.mobile.Sensoriando.Activitys.Configurations.SettingsActivity;
import projetoSaude.mobile.Sensoriando.ProjetoSaudeLib.ConfigurationHelper.ConnectivitySettings;
import projetoSaude.mobile.Sensoriando.ProjetoSaudeLib.BackgroundService;
import projetoSaude.mobile.Sensoriando.ProjetoSaudeLib.ScreenHelpers.Drawer.DrawerHelper;
import projetoSaude.mobile.Sensoriando.enumerated.ConnStatus;
//import org.apache.commons.lang3.StringUtils;

//Exception's Handler


public class MainActivity extends Default {

    //Strings
    private String mConnStatus = "";

    // TextViews
    private TextView tvDisp1;
    private TextView tvDisp2;
    private TextView tvDisp3;
    private TextView tvDisp4;
    private TextView tvDisp5;
    private TextView tvDisp6;

    // Botoes
    private Button btConectar;

    // Objetos
    protected PowerManager.WakeLock mWakeLock;

    // Drawers
    DrawerHelper drawerHelper = null;

    //Coisas do BT
    private BroadcastReceiver broadcastReceiver;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_principal);

        tvDisp1 = (TextView) findViewById(R.id.lbDisp1);
        tvDisp2 = (TextView) findViewById(R.id.lbDisp2);
        btConectar = (Button) findViewById(R.id.btConectar);

        //Preparando o menu
        drawerHelper = new DrawerHelper(this, (DrawerLayout) findViewById(R.id.drawerLayout), (RelativeLayout) findViewById(R.id.drawerPane), (ListView) findViewById(R.id.navList));
        menuPopulator(drawerHelper);

        // Nao permitindo que o celular desligue a tela
        ScreenDontPowerOff();

        // Conexao Bluetooth
        broadcastReceiver = new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                //Update Your UI here..
                String connection = intent.getStringExtra("connection");
                String sEnt = intent.getStringExtra("temp");
                btConectar.setText(connection);
                tvDisp1.setText(sEnt);
            }
        };

        registerReceiver(broadcastReceiver, new IntentFilter(BackgroundService.BROADCAST_ACTION));
    }

    @Override
	public void onBackPressed() {
		AlertDialog.Builder woBuilder = new AlertDialog.Builder(this);
		woBuilder.setMessage(getString(R.string.main_exit_app));
		woBuilder.setCancelable(false);
		woBuilder.setPositiveButton(getString(R.string.main_option_yes),
                new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int id) {
                        Intent intent = new Intent(Intent.ACTION_MAIN);
                        intent.addCategory(Intent.CATEGORY_HOME);
                        intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
                        startActivity(intent);
                    }
                });
		woBuilder.setNegativeButton(getString(R.string.main_option_no),
				new DialogInterface.OnClickListener() {
					public void onClick(DialogInterface dialog, int id) {
						dialog.cancel();
					}
				});
		AlertDialog alert = woBuilder.create();
		alert.show();
	}
    
    @Override
    public void onStart() {
        super.onStart();
    }

    public void onClick(View src) {
        mConnStatus = btConectar.getText().toString().toUpperCase();
        if (src.getId() == R.id.btConectar) {
            switch (ConnStatus.valueOf(mConnStatus)) {
                case CONECTAR:
                    startService(new Intent(this, BackgroundService.class));
                case CONECTANDO:
                    break;
                case DESCONECTADO:
                    stopService(new Intent(this, BackgroundService.class));
                    break;
            }
        }
    }

    private void menuPopulator(DrawerHelper drawerHelper) {
        //Voltar Home
        drawerHelper.createItem(getString(R.string.ic_home), getString(R.string.sub_ic_home), null);

        //Configuracoes - Abrira a SettingsActivity no onClick
        Intent intentConfig = new Intent(this, SettingsActivity.class);
        drawerHelper.createItem(getString(R.string.ic_preferences), getString(R.string.sub_ic_preferences), intentConfig);

        //Sobre
        drawerHelper.createItem(getString(R.string.ic_about), getString(R.string.sub_ic_about), null);
    }

    private void ScreenDontPowerOff() {
        PowerManager pM = (PowerManager) getSystemService(Context.POWER_SERVICE);
        this.mWakeLock = pM.newWakeLock(PowerManager.SCREEN_BRIGHT_WAKE_LOCK, "My Tag");
        this.mWakeLock.acquire();
    }

}