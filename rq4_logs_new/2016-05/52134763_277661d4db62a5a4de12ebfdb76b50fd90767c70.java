package com.emi.nwodcombat.charactercreator.mvp;

import android.app.FragmentManager;

import com.emi.nwodcombat.charactercreator.CharacterCreatorPagerAdapter;
import com.emi.nwodcombat.charactercreator.interfaces.PagerStep;
import com.emi.nwodcombat.charactercreator.mvp.CharacterCreatorView.WizardProgressEvent;
import com.squareup.otto.Subscribe;

import java.util.List;

/**
 * Created by emiliano.desantis on 12/05/2016.
 */
public class CharacterCreatorPresenter {
    private CharacterCreatorView view;
    private CharacterCreatorModel model;
    private CharacterCreatorPagerAdapter adapter;

    public CharacterCreatorPresenter(FragmentManager fragmentManager, CharacterCreatorModel model, CharacterCreatorView view) {
        this.model = model;
        this.view = view;

        this.adapter = new CharacterCreatorPagerAdapter(fragmentManager, getFragmentList());
    }

    private List<PagerStep> getFragmentList() {
        // TODO populate this
        return null;
    }

    public void setUpView() {

    }

    @Subscribe
    public void onWizardProgressEvent(WizardProgressEvent event) {
        int lastPage = adapter.getCount() - 1;
        if (event.isProgressing) {
            if (event.currentItem < lastPage) {
                moveToNextStep();
            } else {
                finishWizard();
            }
        } else {
            moveToPreviousStep();
        }
    }

    public void moveToNextStep() {
        // Save all choices, if any
//        characterCreatorHelper.putAll(fragmentList.get(pager.getCurrentItem()).saveChoices());

        // Move pager forward
        view.pagerGoForward();

//        pager.setCurrentItem(pager.getCurrentItem() + 1);

        // After moving pager forward, if this is a child step, retrieve choices affecting how you distribute points
//        if (fragmentList.get(pager.getCurrentItem()) instanceof PagerStep.ChildStep) {
//            ((PagerStep.ChildStep) fragmentList.get(pager.getCurrentItem())).retrieveChoices();
//        }
    }

    public void moveToPreviousStep() {
        view.pagerGoBackwards();
    }

    private void finishWizard() {
        // What did this do again?
//        pagerFinisher.onPagerFinished();
    }
}