package com.emi.nwodcombat.characterwizard.mvp;

import android.content.Context;

import com.emi.nwodcombat.utils.Events;
import com.squareup.otto.Subscribe;

/**
 * Created by emiliano.desantis on 24/05/2016.
 */
public class SummaryPresenter {
    private final Context context;
    private SummaryView view;
    private CharacterWizardModel model;

    public SummaryPresenter(CharacterWizardModel model, SummaryView view) {
        this.model = model;
        this.view = view;
        this.context = this.view.getContext();
//        setupWidgets();
//        populateView();
    }

    @Subscribe
    public void onWizardComplete(Events.WizardComplete event) {
        populateView();
    }

    private void populateView() {
        view.setAttrSummaryMental(model.getMentalAttrSummary());
        view.setAttrSummaryPhysical(model.getPhysicalAttrSummary());
        view.setAttrSummarySocial(model.getSocialAttrSummary());
        view.setSkillSummaryMental(model.getMentalSkillsSummary());
        view.setSkillSummaryPhysical(model.getPhysicalSkillsSummary());
        view.setSkillSummarySocial(model.getSocialSkillsSummary());
    }

    private void setupWidgets() {
        view.setUpUI();
    }
}