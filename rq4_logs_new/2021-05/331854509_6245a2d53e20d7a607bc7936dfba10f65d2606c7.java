package com.finwin.travancore.traviz.home.contact_us;

import androidx.appcompat.app.AppCompatActivity;
import androidx.databinding.DataBindingUtil;
import androidx.lifecycle.Observer;
import androidx.lifecycle.ViewModelProvider;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.os.Bundle;

import com.finwin.travancore.traviz.R;
import com.finwin.travancore.traviz.databinding.ActivityContactUsBinding;
import com.finwin.travancore.traviz.home.contact_us.action.ContactsAction;
import com.finwin.travancore.traviz.home.contact_us.adapter.ContactsAdapter;
import com.finwin.travancore.traviz.home.transfer.search_beneficiary.SearchBeneficiary;

import cn.pedant.SweetAlert.SweetAlertDialog;

public class ContactUsActivity extends AppCompatActivity {

    ActivityContactUsBinding binding;
    ContactViewModel viewModel;
    ContactsAdapter adapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        binding = DataBindingUtil.setContentView(this, R.layout.activity_contact_us);
        viewModel = new ViewModelProvider(this).get(ContactViewModel.class);
        binding.setViewModel(viewModel);
        setupRecyclerView(binding.rvContacts);

        viewModel.initLoading(this);
        viewModel.getContacts();

        viewModel.getmAction().observe(this, new Observer<ContactsAction>() {
            @Override
            public void onChanged(ContactsAction contactsAction) {
                viewModel.cancelLoading();
                switch (contactsAction.getAction()) {
                    case ContactsAction.GET_CONTAC_SUCCESS:
                        adapter.setContactsDataList(contactsAction.getContactResponse().getData().getTable());
                        adapter.notifyDataSetChanged();
                        break;
                    case ContactsAction.API_ERROR:
                        new SweetAlertDialog(ContactUsActivity.this, SweetAlertDialog.ERROR_TYPE)
                                .setContentText(contactsAction.getError())
                                .setTitleText("Error!")
                                .show();
                        break;
                }
            }
        });
    }

    private void setupRecyclerView(RecyclerView rvContacts) {
        rvContacts.setHasFixedSize(true);
        rvContacts.setLayoutManager(new LinearLayoutManager(this));
        adapter = new ContactsAdapter();
        rvContacts.setAdapter(adapter);
    }
}