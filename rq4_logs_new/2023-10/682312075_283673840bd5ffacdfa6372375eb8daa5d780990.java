package br.udesc.weparty.ui.criarEvento;

import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;
import androidx.lifecycle.ViewModelProvider;

import br.udesc.weparty.databinding.FragmentCriarEventoBinding;

public class CriarEventoFragment extends Fragment {

    private FragmentCriarEventoBinding binding;

    public View onCreateView(@NonNull LayoutInflater inflater,
                             ViewGroup container, Bundle savedInstanceState) {
        CriarEventoViewModel criarEventoViewModel =
                new ViewModelProvider(this).get(CriarEventoViewModel.class);

        binding = FragmentCriarEventoBinding.inflate(inflater, container, false);
        View root = binding.getRoot();
        return root;
    }

    @Override
    public void onDestroyView() {
        super.onDestroyView();
        binding = null;
    }
}