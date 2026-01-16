package com.example.airbnbapi.Model;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotBlank;

public class Game {


        private final String id;
        @NotBlank
        private final String title;
        private final long year;
        @NotBlank
        private final String creator;

        public Game(@JsonProperty("id") String id,
                    @JsonProperty("title") String title,
                    @JsonProperty("year") long year,
                    @JsonProperty("creator") String creator) {
            this.id = id;
            this.title = title;
            this.year = year;
            this.creator = creator;
        }


        public String getId() {
            return id;
        }


        public String getTitle() {
            return title;
        }



        public long getYear() {
            return year;
        }


        public String getCreator() {
            return creator;
        }


        @Override
        public String toString() {
            return
                    "id='" + id + '\'' +
                            ", title='" + title + '\'' +
                            ", year=" + year +
                            ", authors=" + creator +
                            '}';
        }

}