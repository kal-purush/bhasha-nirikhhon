package com.pokedex.koto.pokedex;

/**
 * Created by KOTO on 17/07/2017.
 */

public class PokemonDetail {

    public PokemonDetail() {

    }

    public Form[] getForms() {
        return forms;
    }

    public void setForms(Form[] forms) {
        this.forms = forms;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getWeight() {
        return weight;
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }

    public PokemonDetail(Form[] forms, String name, long weight) {
        this.forms = forms;
        this.name = name;
        this.weight = weight;
    }

    private Form forms[];
    private String name;
    private long weight;


   // private Ability abilities[];
  //  private Stat stats[];
   // private Move moves[];

    public class Form {
        public String getUrl() {
            return url;
        }
        public void setUrl(String url) {
            this.url = url;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        private String url;
        private String name;

        public Form(String url, String name){
            this.url = url;
            this.name = name;
        }
    }



}