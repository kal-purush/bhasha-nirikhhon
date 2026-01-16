package br.com.mowa.timesheet.dialog;

import android.app.Dialog;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v4.app.DialogFragment;
import android.support.v7.app.AlertDialog;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.ListView;

import java.util.ArrayList;
import java.util.List;

import br.com.mowa.timesheet.activity.TasksActivity;
import br.com.mowa.timesheet.model.ProjectModel;
import br.com.mowa.timesheet.timesheet.R;
import br.com.mowa.timesheet.utils.SharedPreferencesUtil;

/**
 * Created by walky on 10/28/15.
 */
public class ProfileMenuFilterDialogFragment extends DialogFragment {
    private List<ProjectModel> listProject;
    private ListView lvMenu;

    @NonNull
    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());
        LayoutInflater inflater = getActivity().getLayoutInflater();
        View view = inflater.inflate(R.layout.fragment_profile_filter, null);
        listProject = SharedPreferencesUtil.getListProjectFromSharedPreferences();
        this.lvMenu = (ListView) view.findViewById(R.id.fragment_profile_filter_list_view);
        this.lvMenu.setAdapter(new ArrayAdapter<String>(getContext(), android.R.layout.simple_list_item_1, getList()));
        this.lvMenu.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                TasksActivity activity = (TasksActivity) getActivity();
                if (listProject.size() == position) {

                } else {
                    activity.filterProject(listProject.get(position));
                    dismiss();
                }
            }
        });




        builder.setView(view);
        return builder.create();
    }


    private List<String> getList() {
        List<String> list = new ArrayList<>();
        for (ProjectModel project : listProject) {
            list.add(project.getName());
        }
        list.add("Todos os projetos");

        return list;
    }
}