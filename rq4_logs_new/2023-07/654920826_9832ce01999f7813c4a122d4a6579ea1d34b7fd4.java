package org.tms.ui.services;

import io.qameta.allure.Step;
import org.tms.ui.pages.CasePage;

public class CasePageService {
    private CasePage casePage = new CasePage();

    public boolean isSaveButtonDisplayed() {
        return casePage.isSaveButtonDisplayed();
    }
}