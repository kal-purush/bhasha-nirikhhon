package policia.transito;

import java.util.HashMap;

import policia.transito.data.BaseFiscalizacao;
import policia.transito.data.DefaultDatas;
import policia.transito.model.Agente;
import policia.transito.model.Device;
import socket.listener.ClienteService;
import socket.listener.OnReciveTranfer;
import socket.modelo.Transfer;
import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

public class ActiveUserActivity extends Activity implements DefaultDatas, OnClickListener, OnReciveTranfer
{
	private EditText novaSenha;
	private EditText confSenha;
	private Button btAtivar;
	private int idUser;
	private ClienteService service;
	private BaseFiscalizacao baseFiscalizacao;
	
	@Override
	protected void onCreate(Bundle savedInstanceState)
	{
		super.onCreate(savedInstanceState);
		setContentView(R.layout.layout_user_ativate);
		init();
	}
	
	private void init()
	{
		this.baseFiscalizacao = new BaseFiscalizacao(this);
		this.service = ClienteService.findCurrentConnection();
		service.setOnTransitActivity(this, this);
		
		
		this.btAtivar = (Button) findViewById(R.id.ativarUtilizadorBotao);
		this.novaSenha = (EditText) findViewById(R.id.ativarUtilizadorNovaSenha);
		this.confSenha = (EditText) findViewById(R.id.ativarUtilizadorConfirmarSenha);
		this.btAtivar.setOnClickListener(this);
	}
	
	@Override
	public void onClick(View click)
	{
		if(click.equals(btAtivar))
		{
			int filled = 1;
			Device device = new Device(this);
			Transfer transfer = new Transfer(Agente.getSender(this, this), SERVER_APP_NAME, 7003, socket.modelo.Transfer.Intent.VALIDATE, "Peti��o Ativar utilizador");
			if(this.novaSenha.getText().toString().equals(""))
			{
				Toast.makeText(this, "Insira a nova palavra-passe.", Toast.LENGTH_LONG).show();
				filled =0;
			}
			else
			{
				if(this.confSenha.getText().toString().equals(""))
				{
					Toast.makeText(this, "Confirme a nova palavra-passe.", Toast.LENGTH_LONG).show();
					filled =0;
				}
			}
			if(filled ==1)
			{
				if(!this.novaSenha.getText().toString().equals(this.confSenha.getText().toString()))
				{
					this.confSenha.setText("");
					Toast.makeText(this, R.string.senhasDiferentes, Toast.LENGTH_LONG).show();
				}
				else
				{
					HashMap<String, String> map = new HashMap<String, String>();
					map.put("ID USER", baseFiscalizacao.getAgente().getId()+"");
					map.put("PWD", this.novaSenha.getText().toString());
					transfer.getListMaps().add(map);
					transfer.setEspera(Espera.ACTIVAR_UTILIZADOR.name());
					service.transfer(transfer);				
				}
				
			}
		}
		
	}

	@Override
	public void onProcessTranfer(Transfer transfer) 
	{
		Transfer data = transfer;
		
		Espera espera = Espera.valueOf(data.getEspera());
		if(espera == Espera.ACTIVAR_UTILIZADOR)
		{
			
			if(data != null)
			{
				if(data.getListMaps().get(0).get("RESULTADO").equals("ativado"))
				{
					Intent intent = new Intent(this, ActivityInitial.class);
					startActivity(intent);
				}
				else Toast.makeText(this, data.getListMaps().get(0).get("RESULTADO"), Toast.LENGTH_LONG).show();
			}
		}
		
	}
	
	private enum Espera
	{
		ACTIVAR_UTILIZADOR
	}

	
	@Override
	public void onPosConnected()
	{
		
	}

	@Override
	public void onConnectLost() {
		// TODO Auto-generated method stub
		
	}
	
}