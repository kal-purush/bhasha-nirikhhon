package com.example.gocourse;

import android.widget.LinearLayout;
import androidx.lifecycle.LiveData;
import androidx.lifecycle.MutableLiveData;
import androidx.lifecycle.ViewModel;
import com.example.gocourse.ui.fragments.UserFragment;

public class SharedViewModel extends ViewModel {
    private LinearLayout quickActionsMenu;
    private final MutableLiveData<Boolean> quickMenuVisible = new MutableLiveData<>(false);

    public void setQuickActionsMenu(LinearLayout menu) {
        this.quickActionsMenu = menu;
    }

    public LinearLayout getQuickActionsMenu() {
        return quickActionsMenu;
    }

    public void setQuickMenuVisible(boolean visible) {
        quickMenuVisible.setValue(visible);
    }

    public LiveData<Boolean> getQuickMenuVisible() {
        return quickMenuVisible;
    }

    public void toggleQuickMenu() {
        Boolean currentValue = quickMenuVisible.getValue();
        quickMenuVisible.setValue(currentValue == null || !currentValue);
    }
} 