package com.emi.nwodcombat.characterwizard;

import android.app.Fragment;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.emi.nwodcombat.R;
import com.emi.nwodcombat.charactercreator.CharacterCreatorHelper;
import com.emi.nwodcombat.charactercreator.interfaces.PagerFinisher;
import com.emi.nwodcombat.charactercreator.interfaces.PagerStep;
import com.emi.nwodcombat.characterwizard.mvp.CharacterCreatorModel;
import com.emi.nwodcombat.characterwizard.mvp.CharacterCreatorPresenter;
import com.emi.nwodcombat.characterwizard.mvp.CharacterCreatorView;
import com.emi.nwodcombat.utils.BusProvider;

import java.util.List;

import butterknife.ButterKnife;

/**
 * Created by emiliano.desantis on 13/05/2016.
 */
public class CharacterWizardFragment extends Fragment {
    //    CharacterCreatorPagerAdapter adapter;
    List<PagerStep> fragmentList;
    CharacterCreatorHelper characterCreatorHelper;
    PagerFinisher pagerFinisher;

    private CharacterCreatorPresenter presenter;

    public static CharacterWizardFragment newInstance(List<PagerStep> fragments, PagerFinisher pagerFinisher) {
        CharacterWizardFragment fragment = new CharacterWizardFragment();
        fragment.characterCreatorHelper = CharacterCreatorHelper.getInstance();
        fragment.fragmentList = fragments;
        fragment.pagerFinisher = pagerFinisher;
        return fragment;
    }

    @Override
    public void onActivityCreated(Bundle savedInstanceState) {
        super.onActivityCreated(savedInstanceState);
        createPresenter();
    }

    @Override
    public void onResume() {
        super.onResume();
        BusProvider.register(presenter);
    }

    @Override
    public void onPause() {
        BusProvider.unregister(presenter);
        super.onPause();
    }


    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        return inflater.inflate(R.layout.fragment_pager, container, false);
    }

    @Override
    public void onDestroyView() {
        super.onDestroyView();
        ButterKnife.unbind(this);
    }

//    @Override
//    public void onCreate(Bundle savedInstanceState) {
//        super.onCreate(savedInstanceState);
//        setRetainInstance(true);
//        setHasOptionsMenu(true);
//        this.adapter = new CharacterCreatorPagerAdapter(getChildFragmentManager(), fragmentList);
//    }

//    @Override
//    public void onViewCreated(View view, Bundle savedInstanceState) {
//        pager.setAdapter(adapter);
//    }


//    @Override
//    public void checkStepIsComplete(final boolean isComplete, PagerStep caller) {
//        if (isComplete) {
//            btnNext.setEnabled(true);
//            btnPrevious.setEnabled(true);
////            characterCreatorHelper.putAll(caller.saveChoices());
//        } else {
//            btnNext.setEnabled(false);
//        }
//    }

//    @Override
//    public long commitChoices(Character character) {
//        controller = CharacterController.getInstance(getContext());
//
//        long result = controller.save(character);
//
//        Log.d("Character creator", String.valueOf(result));
//
//        ((NavDrawerActivity) getActivity()).onCharacterCreatorFinish();
//
//        return result;
//    }

    private void createPresenter() {
        presenter = new CharacterCreatorPresenter(getChildFragmentManager(), new CharacterCreatorModel(getActivity()), new CharacterCreatorView(this,
            BusProvider.getInstance()));
        presenter.setUpView();
    }

//    public void moveToNextStep() {
//        characterCreatorHelper.putAll(fragmentList.get(pager.getCurrentItem()).saveChoices());
//
//        pager.setCurrentItem(pager.getCurrentItem() + 1);
//        int lastPage = pager.getAdapter().getCount() - 1;
//
//        if (fragmentList.get(pager.getCurrentItem()) instanceof PagerStep.ChildStep) {
//            ((PagerStep.ChildStep) fragmentList.get(pager.getCurrentItem())).retrieveChoices();
//        }
//
//        if (pager.getCurrentItem() == lastPage) {
//            btnNext.setText(getString(R.string.button_finish));
//        } else {
//            btnNext.setText(getString(R.string.button_next));
//        }
//    }
//
//    public void moveToPreviousStep() {
//        if (pager.getCurrentItem() != 0) {
//            pager.setCurrentItem(pager.getCurrentItem() - 1);
//        } else {
//            getFragmentManager().popBackStack();
//        }
//    }

//    private void finishWizard() {
//        pagerFinisher.onPagerFinished();
//    }

}