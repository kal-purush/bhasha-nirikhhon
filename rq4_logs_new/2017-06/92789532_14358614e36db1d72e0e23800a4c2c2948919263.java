package com.dell.juliana.filme.fragments;

import android.content.Intent;
import android.content.res.Configuration;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentTransaction;
import android.support.v7.widget.Toolbar;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ListView;

import com.dell.juliana.filme.FormFilmeActivity;
import com.dell.juliana.filme.R;
import com.dell.juliana.filme.dao.FilmeDAO;
import com.dell.juliana.filme.model.Filme;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Juliana on 18/06/2017.
 */

public class ListFilmesFragments extends Fragment {

    private ListView etFilmes;
    private ArrayAdapter<String> adapter;
    private Toolbar toolbarLayout;


    List<Filme> filmes = new ArrayList<Filme>();

    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.fragment_list_filme, container, false);
        toolbarLayout = (Toolbar)view.findViewById(R.id.toolbar);


        //PEGAR REFERÊNCIA DO LISTVIEW E DO BUTTON
        etFilmes = (ListView)view.findViewById(R.id.etFilmes);
        Button btnAddLista = (Button)view.findViewById(R.id.btnAddLista);

        loadFilmes();

        etFilmes.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {

                if (isLandScape()){
                    loadFilmeForm(filmes.get(position));
                }else {
                    loadDetalheJson(filmes.get(position));
                }
            }
        });

        //ADICIONAR COMPORTAMENTO PARA O BOTÃO
        btnAddLista.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                if (isLandScape()){
                    loadFilmeForm(null);
                }else {
                    Intent it = new Intent(getActivity(), FormFilmeActivity.class);
                    startActivity(it);
                }
            }
        });

        return view;
    }

    /**
     *
     * @param filme Recebe a equipe selecionada na lista
     */
    private void loadDetalheJson(Filme filme){
        FragmentManager manager = getActivity().getSupportFragmentManager();
        FragmentTransaction transaction = manager.beginTransaction();
        Fragment fragment = new DetalheFilmesFragments();
        if (filme != null) {
            Bundle bundle = new Bundle();
            bundle.putSerializable("filme", filme);
            fragment.setArguments(bundle);
        }
        transaction.replace(R.id.fragmentList, fragment);
        transaction.addToBackStack(null);
        transaction.commit();
    }

    //Listando Equipe no form (Tablet)
    private void loadFilmeForm(Filme filme) {
        FragmentManager manager = getActivity().getSupportFragmentManager();
        FragmentTransaction transaction = manager.beginTransaction();
        Fragment fragment = new FilmeFormFragments();
        if (filme != null) {
            Bundle bundle = new Bundle();
            bundle.putSerializable("filme", filme);
            fragment.setArguments(bundle);
        }
        transaction.replace(R.id.fragment_filme_form, fragment);
        transaction.addToBackStack(null);
        transaction.commit();
    }


    //Verificar se é landscape
    public boolean isLandScape(){
        Configuration configuration = getResources().getConfiguration();
        if (configuration.orientation == Configuration.ORIENTATION_LANDSCAPE)
            return true;
        return false;
    }

    //Retornar filmes para lista depois de gravada
    public void loadFilmes() {
        FilmeDAO dao = new FilmeDAO(getActivity());
        filmes = dao.pegarTodosFilmes();

        List<String> filmesTitulo = new ArrayList<String>();

        for (Filme filme:filmes){
            filmesTitulo.add(filme.getTitulo());
        }
        adapter = new ArrayAdapter<String>(
                getActivity(),
                android.R.layout.simple_list_item_1,
                filmesTitulo);
        etFilmes.setAdapter(adapter);
    }

    @Override
    public void onStart() {
        super.onStart();
        /*Estou notificando todos que tem esse meu adapter que agora mudei de estado e agora eles vão ter que fazer alguma coisa,
        * essa alguma coisa na implementação do ListView  é carregar de novo*/
        //adapter.notifyDataSetChanged();
        loadFilmes();
    }

    /*public interface CliqueNaFilmeListener{
        void filmeFoiClicada(Filme filme);
    }*/
}




