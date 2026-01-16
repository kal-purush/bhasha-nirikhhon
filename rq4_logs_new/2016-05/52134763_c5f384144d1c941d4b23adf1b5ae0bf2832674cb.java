package com.emi.nwodcombat.characterwizard.mvp;

import android.content.Context;

import com.emi.nwodcombat.adapters.DemeanorsAdapter;
import com.emi.nwodcombat.adapters.NaturesAdapter;
import com.emi.nwodcombat.adapters.VicesAdapter;
import com.emi.nwodcombat.adapters.VirtuesAdapter;
import com.emi.nwodcombat.model.realm.Demeanor;
import com.emi.nwodcombat.model.realm.Nature;
import com.emi.nwodcombat.model.realm.Vice;
import com.emi.nwodcombat.model.realm.Virtue;

import io.realm.RealmResults;

/**
 * Created by emiliano.desantis on 18/05/2016.
 */
public class PersonalInfoPresenter {
    private final Context context;
    private PersonalInfoView view;
    private CharacterWizardModel model;

    private DemeanorsAdapter demeanorsAdapter;

    private VicesAdapter vicesAdapter;

    private NaturesAdapter naturesAdapter;

    private VirtuesAdapter virtuesAdapter;

    public PersonalInfoPresenter(Context context, CharacterWizardModel model, PersonalInfoView view) {
        this.model = model;
        this.view = view;
        this.context = view.getContext();
        setupWidgets();
    }

    private void setupWidgets() {
        setupDemeanorsSpinner();
        setupNaturesSpinner();
        setupVicesSpinner();
        setupVirtuesSpinner();

        view.setUpTextWatcher();
        view.setupTestData();
    }

    public void setupDemeanorsSpinner() {
        RealmResults<Demeanor> demeanors = model.getDemeanors();

        demeanorsAdapter = new DemeanorsAdapter(context, demeanors, true);

        view.setDemeanorsSpinnerAdapter(demeanorsAdapter);
    }

    public void setupNaturesSpinner() {
        RealmResults<Nature> natures = model.getNatures();

        naturesAdapter = new NaturesAdapter(context, natures, true);

        view.setNaturesSpinnerAdapter(naturesAdapter);
    }

    public void setupVicesSpinner() {
        RealmResults<Vice> vices = model.getVices();

        vicesAdapter = new VicesAdapter(context, vices, true);

        view.setVicesSpinnerAdapter(vicesAdapter);
    }

    public void setupVirtuesSpinner() {
        RealmResults<Virtue> virtues = model.getVirtues();

        virtuesAdapter = new VirtuesAdapter(context, virtues, true);

        view.setVirtuesSpinnerAdapter(virtuesAdapter);
    }

}