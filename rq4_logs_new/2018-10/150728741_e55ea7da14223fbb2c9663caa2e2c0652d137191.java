package com.daahae.damoyeo.presenter;

import com.daahae.damoyeo.view.adapter.BuildingAdapter;
import com.daahae.damoyeo.view.fragment.CategoryFragment;

public class CategoryFragmentPresenter {
    private CategoryFragment view;

    private BuildingAdapter buildingAdapter;

    public CategoryFragmentPresenter(CategoryFragment view) {
        this.view = view;
    }

    public void setBuildingInfo(BuildingAdapter buildingAdapter){
        this.buildingAdapter = buildingAdapter;

        buildingAdapter.resetList();
        makeDummy();

    }

    //TODO: 삭제예정
    private void makeDummy(){
        for(int i=0;i<3;i++){
            buildingAdapter.addDummy();
        }
    }
}