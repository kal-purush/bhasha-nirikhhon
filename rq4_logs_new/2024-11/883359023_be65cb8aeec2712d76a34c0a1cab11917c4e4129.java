package interface_adapter.profile;

import use_case.profile.ProfileInputBoundary;
import use_case.profile.ProfileInputData;

public class ProfileController {

    private final ProfileInputBoundary profileInputBoundary;

    public ProfileController(ProfileInputBoundary profileInputBoundry) {
        this.profileInputBoundary = profileInputBoundry;
    }

//    public void execute(String username) {
//
//        this.profileInputBoundary.execute();
//    }

    public void switchToSavedRecipesView() {
        this.profileInputBoundary.switchToSavedRecipesView();
    }
}