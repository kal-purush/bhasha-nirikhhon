package com.fiuba.tdpii.correapp.models.web;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class DriverRating {

        @SerializedName("rating")
        @Expose
        private Long rating;
        @SerializedName("comment")
        @Expose
        private String comment;

        public Long getRating() {
            return rating;
        }

        public void setRating(Long rating) {
            this.rating = rating;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }
}