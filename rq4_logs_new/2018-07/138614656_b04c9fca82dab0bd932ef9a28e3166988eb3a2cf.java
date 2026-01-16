package com.andela.javadevelopers.contract;

import com.andela.javadevelopers.model.GithubUsers;


import java.util.List;

/**
 * Created by andeladeveloper on 10/07/2018.
 */
public interface MainContract {
    /**
     * The interface Main view.
     */
    interface MainView {
        /**
         * Display dev list.
         *
         * @param githubUsers the github users
         */
        void displayDevList(List<GithubUsers> githubUsers);

        /**
         * Show loader.
         */
        void showLoader();

        /**
         * Hide loader.
         */
        void hideLoader();
    }

    /**
     * The interface Main presenter.
     */
    interface MainPresenter {
        /**
         * Query api github presenter.
         *
         */
        void queryApi();
    }
}