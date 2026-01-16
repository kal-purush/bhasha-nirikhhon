package com.ufo.ufomobile.reportesmoviles;

import android.app.Activity;
import android.app.Dialog;
import android.content.Context;
import android.os.Bundle;
import android.support.v4.app.DialogFragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.view.Window;
import android.widget.AdapterView;
import android.widget.BaseAdapter;
import android.widget.ImageView;
import android.widget.ListView;
import android.widget.TextView;

import java.util.ArrayList;
import java.util.List;

import utilities.Category;

/**
 * Created by 'Santiago on 31/7/2016.
 */
public class CategorySelectionDialogFragment extends DialogFragment {

    OnaAddSelected mListener;
    ListView categories;
    ArrayList<Category> categoryList;

    public interface OnaAddSelected {
        public void onArticleSelectedListener(int resource, String name);
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout to use as dialog or embedded fragment
        View view=inflater.inflate(R.layout.category_selection_dialog, container, false);
        categoryList=new ArrayList<Category>();
        categoryList.add(new Category("Alcantarillado",R.drawable.alcantarillado));
        categoryList.add(new Category("Alumbrado",R.drawable.alumbrado));
        categoryList.add(new Category("Acueducto",R.drawable.acueducto));
        categoryList.add(new Category("Basura",R.drawable.basura));
        categoryList.add(new Category("Limpieza",R.drawable.limpieza));
        categoryList.add(new Category("Gas domiciliario",R.drawable.gas));

        categories=(ListView)view.findViewById(R.id.categories);
        categories.setAdapter(new ListAdapter(getActivity(), categoryList));

        categories.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> adapterView, View view, int i, long l) {
                Category cat = categoryList.get(i);
                String name=cat.getName();
                int resource=cat.getResource();
                mListener.onArticleSelectedListener(resource,name);
                dismiss();
            }
        });
        return view;
    }

    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        // The only reason you might override this method when using onCreateView() is
        // to modify any dialog characteristics. For example, the dialog includes a
        // title by default, but your custom layout might not need it. So here you can
        // remove the dialog title, but you must call the superclass to get the Dialog.
        Dialog dialog = super.onCreateDialog(savedInstanceState);
        dialog.requestWindowFeature(Window.FEATURE_NO_TITLE);
        return dialog;
    }

    @Override
    public void onAttach(Activity activity) {
        super.onAttach(activity);
        try {
            mListener = (OnaAddSelected) activity;
        } catch (ClassCastException e) {
            throw new ClassCastException(activity.toString() + " must implement OnArticleSelectedListener");
        }
    }

    //----------------------------------------------------------------------------------------------------------------------------
    public class ListAdapter extends BaseAdapter {

        private Context context;
        private List<Category> list;

        public ListAdapter(Context context, List<Category> list) {
            this.context = context;
            this.list = list;
        }

        @Override
        public int getCount() {
            return list.size();
        }

        @Override
        public Object getItem(int position) {
            return list.get(position);
        }

        @Override
        public long getItemId(int position) {
            return position;
        }

        @Override
        public View getView(int position, View convertView, ViewGroup parent) {
            View rowView = convertView;

            if (convertView == null) {
                // Create a new view into the list.
                LayoutInflater inflater = (LayoutInflater) context
                        .getSystemService(Context.LAYOUT_INFLATER_SERVICE);
                rowView = inflater.inflate(R.layout.category_selection_item, parent, false);
            }

            // Set data into the view.
            TextView name = (TextView) rowView.findViewById(R.id.name);
            ImageView resource = (ImageView) rowView.findViewById(R.id.resource);

            Category item = this.list.get(position);
            name.setText(item.getName());
            resource.setImageResource(item.getResource());
            return rowView;
        }
    }
}