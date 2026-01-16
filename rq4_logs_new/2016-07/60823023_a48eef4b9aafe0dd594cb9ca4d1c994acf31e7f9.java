package br.com.ufg.inf.horacerta;

import android.app.NotificationManager;
import android.content.Context;
import android.content.Intent;
import android.support.design.widget.FloatingActionButton;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.TextView;

import dao.MedicamentoDAO;
import model.Medicamento;

public class AlarmeActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_alarme);

        MedicamentoDAO medicamentoDAO = new MedicamentoDAO(getApplicationContext());
        Medicamento medicamento = medicamentoDAO.findById(getIntent().getExtras().getString("id"));

        if(medicamento != null) {
            TextView nomeMedicamento = (TextView) findViewById(R.id.nomeMedicamentoAlarme);
            TextView descricaoDeUso = (TextView) findViewById(R.id.descricaoMedicamentoAlarme);
            nomeMedicamento.setText(medicamento.getNome());
            descricaoDeUso.setText(medicamento.getDescricaoDoUso());
        }

        FloatingActionButton fabPausarNotificacao = (FloatingActionButton) findViewById(R.id.fabPausarNotificacao);
        fabPausarNotificacao.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                NotificationManager mNotificationManager = (NotificationManager) getApplicationContext().getSystemService(Context.NOTIFICATION_SERVICE);
                mNotificationManager.cancelAll();
                Intent intent = new Intent(getApplicationContext(), MainActivity.class);
                intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TASK);
                startActivity(intent);
                finish();
            }
        });
    }
}