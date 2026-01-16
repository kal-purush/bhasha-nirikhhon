/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package owe6.eindopdrachtlottehouwen;

/**
 *
 * @author lotte
 */
public class Virus {
 public Virus(int virus_id, String virusnaam, String virusclasse, int hostid, String hostnaam, String hostclasse) {
        this.virus_id = virus_id;
        this.virusnaam = virusnaam;
        this.virusclasse = virusclasse;     
        this.hostid = hostid;    
    }
        private int virus_id; 
        private String virusnaam;
        private String virusclasse;      
        private int hostid; 


    Virus() {}
/*
    Virus(String[] virusarray){
        this.virus_id = virusarray[0];
        this.virusnaam = virusarray[1];
        this.virusclasse = virusarray[2];
        this.refseqid = virusarray[3];
        
        this.kegg_genome = virusarray[4];
        this.kegg_disease = virusarray[5];
        this.disease = virusarray[6];
        
        this.hostid = virusarray[7];
        this.hostnaam = virusarray[8];
        this.hostclasse = virusarray[9];
        this.pmid = virusarray[10];
        this.evidence = virusarray[11];}
    }
*/

    public int getVirus_id() {
        return virus_id;
    }

    public void setVirus_id(int virus_id) {
        this.virus_id = virus_id;
    }

    public String getVirusnaam() {
        return virusnaam;
    }

    public void setVirusnaam(String virusnaam) {
        this.virusnaam = virusnaam;
    }

    public String getVirusclasse() {
        return virusclasse;
    }

    public void setVirusclasse(String virusclasse) {
        this.virusclasse = virusclasse;
    }

    public int getHostid() {
        return hostid;
    }

    public void setHostid(int hostid) {
        this.hostid = hostid;
    }
        
        
}