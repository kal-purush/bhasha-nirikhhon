package com.emi.nwodcombat.characterwizard.mvp;

import android.content.Context;

import com.emi.nwodcombat.R;
import com.emi.nwodcombat.model.realm.Entry;
import com.emi.nwodcombat.utils.Constants;
import com.emi.nwodcombat.utils.Events;
import com.squareup.otto.Subscribe;

/**
 * Created by emiliano.desantis on 23/05/2016.
 * Presenter for third step of character creator done in MVP.
 */
public class SkillSettingPresenter {
    private final Context context;
    private SkillSettingView view;
    private CharacterWizardModel model;

    public SkillSettingPresenter(CharacterWizardModel model, SkillSettingView view) {
        this.model = model;
        this.view = view;
        this.context = view.getContext();
        setupWidgets();
    }

    private void setupWidgets() {
        view.setUpUI();
    }

    @Subscribe
    public void onEntryChanged(Events.SkillChanged event) {
        int spent = 0;
        switch (event.category) {
            case Constants.CONTENT_DESC_SKILL_MENTAL: {
                spent = model.getPointsSpentOnMental();

                break;
            }
            case Constants.CONTENT_DESC_SKILL_PHYSICAL: {
                spent = model.getPointsSpentOnPhysical();

                break;
            }
            case Constants.CONTENT_DESC_SKILL_SOCIAL: {
                spent = model.getPointsSpentOnSocial();

                break;
            }
        }

        changeValue(event.isIncrease, event.key, event.category, spent);

        view.checkCompletionConditions();
    }

    private void changeValue(boolean isIncrease, String key, String category, int spent) {
        Integer change = isIncrease ? 1 : -1;

        if (!model.isCheating()) {

            int modelEntryValue = model.findEntryValue(key, 1);

            if ((change > 0 && spent < Constants.SKILL_PTS_PRIMARY) || (change < 0 && spent > 0)) {
                change += modelEntryValue;

                Entry entry = new Entry().setKey(key).setType(Constants.FIELD_TYPE_INTEGER).setValue(change);

                view.changeWidgetValue(key, Integer.valueOf(model.addOrUpdateEntry(entry).getValue()));
                spent += isIncrease ? 1 : -1;

                setCategoryTitle(spent, category);
            }
        }
        else    // If point allocation is not limited by category, do this instead
        {
            view.changeWidgetValue(key, change);
        }
    }

    private void setCategoryTitle(int spent, String category) {
        switch (category) {
            case Constants.CONTENT_DESC_SKILL_MENTAL: {

                view.setMentalCategoryTitle(spent, context.getString(R.string.cat_mental));

                break;
            }
            case Constants.CONTENT_DESC_SKILL_PHYSICAL: {

                view.setPhysicalCategoryTitle(spent, context.getString(R.string.cat_physical));

                break;
            }
            case Constants.CONTENT_DESC_SKILL_SOCIAL: {

                view.setSocialCategoryTitle(spent, context.getString(R.string.cat_social));

                break;
            }
        }
    }
}