package com.example.planties.features.plant_care;

import com.example.planties.core.GardenType;
import com.example.planties.features.utils.adapter.filter.FilterModel;

public abstract class GardenViewEvent {
    public static class OnRefresh extends GardenViewEvent { }
    public static class OnChangedType extends GardenViewEvent {
        private String type;

        public OnChangedType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
        public void setType(String type) {
            this.type = type;
        }
    }
    public static class OnChangedSorting extends GardenViewEvent {
        private String sorting;

        public OnChangedSorting(String sorting) {
            this.sorting = sorting;
        }

        public String getSorting() {
            return sorting;
        }
        public void setSorting(String sorting) {
            this.sorting = sorting;
        }
    }
}