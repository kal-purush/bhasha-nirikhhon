package Database;

import androidx.lifecycle.MutableLiveData;
import androidx.lifecycle.ViewModel;

public class SharedViewModel extends ViewModel {
    private final MutableLiveData<String> selectItem = new MutableLiveData<String>();


    public void setData(String item) {
        selectItem.setValue(item);
    }


    public MutableLiveData<String> getSelectItem() {
        return selectItem;
    }

}