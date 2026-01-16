package com.emi.nwodcombat.characterwizard.mvp;

import android.content.Context;
import android.widget.TextView;

import com.emi.nwodcombat.R;
import com.emi.nwodcombat.utils.Constants;
import com.emi.nwodcombat.utils.Events;
import com.emi.nwodcombat.widgets.ValueSetter;
import com.squareup.otto.Subscribe;

/**
 * Created by emiliano.desantis on 19/05/2016.
 */
public class AttrSettingPresenter {
    private final Context context;
    private AttrSettingView view;
    private CharacterWizardModel model;

    public AttrSettingPresenter(Context context, CharacterWizardModel model, AttrSettingView view) {
        this.model = model;
        this.view = view;
        this.context = view.getContext();
        setupWidgets();
    }

    private void setupWidgets() {
        view.setUpUI();
    }

    @Subscribe
    public void onTraitChanged(Events.TraitChanged event) {
        ValueSetter widget = (ValueSetter) event.caller;

        TextView txtPoolMental = (TextView) event.categoryTitles[0];
        TextView txtPoolPhysical = (TextView) event.categoryTitles[1];
        TextView txtPoolSocial = (TextView) event.categoryTitles[2];

        // TODO Model calls for saving data should be made here for each case
        switch (widget.getTraitCategory()) {
            case Constants.CONTENT_DESC_ATTR_MENTAL: {
                int spent = view.getPointsSpentOnMental();

                spent = changeWidgetValue(widget, spent, event.value, model.isCheating());

                view.setMentalCategoryTitle(spent, context.getString(R.string.cat_mental));

                break;
            }
            case Constants.CONTENT_DESC_ATTR_PHYSICAL: {
                int spent = view.getPointsSpentOnPhysical();

                spent = changeWidgetValue(widget, spent, event.value, model.isCheating());

                view.setPhysicalCategoryTitle(spent, context.getString(R.string.cat_physical));

                break;
            }
            case Constants.CONTENT_DESC_ATTR_SOCIAL: {
                int spent = view.getPointsSpentOnSocial();

                spent = changeWidgetValue(widget, spent, event.value, model.isCheating());

                view.setSocialCategoryTitle(spent, context.getString(R.string.cat_social));

                break;
            }
        }

        view.checkCompletionConditions();
    }

    private int changeWidgetValue(ValueSetter widget, int spent, int value, boolean cheating) {
        if (!cheating) {
            if (value > 0) {
                if (spent < Constants.ATTR_PTS_PRIMARY) {
                    view.changeWidgetValue(widget, value);
                    spent += value;
                }
            } else {
                if (spent > 0) {
                    view.changeWidgetValue(widget, value);
                    spent += value;
                }
            }
        }
        else    // If point allocation is not limited by category, do this instead
        {
            if (value > 0) {
                view.changeWidgetValue(widget, value);
            } else {
                view.changeWidgetValue(widget, value);
            }
        }

        return spent;
    }
}