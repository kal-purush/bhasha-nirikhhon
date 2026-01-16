package fr.cg44.plugin.archives.importation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.fileupload.FileItem;
import org.apache.log4j.Logger;

import com.csvreader.CsvReader;
import com.jalios.jcms.Category;
import com.jalios.jcms.Channel;
import com.jalios.jcms.ControllerStatus;
import com.jalios.jcms.FileDocument;
import com.jalios.jcms.Publication;
import com.jalios.jcms.context.JcmsMessage;
import com.jalios.util.Util;

import fr.cg44.plugin.socle.SocleUtils;
import generated.City;
import generated.Periodique;

public class ImportPeriodiqueFromCsv {
    
    private static CsvReader csvReader;
    private static final char SEPARATOR = ';';
    private static final String doubleHashtag = "##";
    private static final Logger LOGGER = Logger.getLogger(ImportPeriodiqueFromCsv.class);
    private static String encoding = "UTF-8";
    protected static final Channel channel = Channel.getChannel();
    
    /*
     * Liste des valeurs attendues dans le CSV
     * 0 Titre - String - obligatoire
     * 1 Image bandeau - String
     * 2 Copyright - String
     * 3 Légende - String
     * 4 Texte alternatif - String
     * 5 Cote de périodique - String - obligatoire
     * 6 Dates extrêmes de la collection - String - Obligatoire
     * 7 Dates extrêmes de la collection numérisée - String
     * 8 Inventaire de la collection - FileDocument
     * 9 Fréquence parution - Category - Obligatoire
     * 10 Ancien titre - Periodique[]
     * 11 Titre devenu - Periodique[]
     * 12 Notes - String
     * 13 Siège du journal - City
     * 14 Numérisé - boolean oui/non
     * 15 Thème - Category[] - Obligatoire
     * 16 Tendances politiques - Category[]
     * 17 Période - Category[] - Obligatoire
     * 18 Communes concernées - City[]
     * 19 Toutes les communes du département - boolean oui/non
     */
    
    /**
     * Va renvoyer différents logs informatifs pour informer l'utilisateur
     * de l'état du CSV et des lignes en erreur
     * sans effectuer d'import de données
     */
    public static Map<String, String> checkCsvImport(FileItem fileItem) {
        Map<String, String> returnedLog = new TreeMap<>();
        
        if (!fileIsNotCorrectCsv(fileItem)) {
            try {
                csvReader = new CsvReader(new InputStreamReader(fileItem.getInputStream(),encoding), SEPARATOR);
                String[] values;
                int cpt = 2;
                StringBuilder lineLog;
                boolean skippedFirst = false;
                while (csvReader.readRecord()){
                    /*
                     * Champs à surveiller :
                     * 
                     * 0 Titre - String - obligatoire
                     * 5 Cote de périodique - String - obligatoire
                     * 6 Dates extrêmes de la collection - String - Obligatoire
                     * 8 Inventaire de la collection - FileDocument
                     * 9 Fréquence parution - Category - Obligatoire
                     * 10 Ancien titre - Periodique[]
                     * 11 Titre devenu - Periodique[]
                     * 13 Siège du journal - City
                     * 15 Thème - Category[] - Obligatoire
                     * 16 Tendances politiques - Category[]
                     * 17 Période - Category[] - Obligatoire
                     * 18 Communes concernées - City[]
                     */
                    lineLog = new StringBuilder();
                    values = csvReader.getValues();
                    if (values.length != 20) {
                        channel.getCurrentJcmsContext().addMsgSession(new JcmsMessage(JcmsMessage.Level.ERROR, "Le fichier CSV doit avoir 20 colonnes, et il n'en a que " + values.length));
                        break;
                    }
                    if (!skippedFirst) { // ne pas lire la ligne des headers
                        skippedFirst = true;
                        continue;
                    }
                    if (Util.isEmpty(values[0])) { // titre
                        lineLog.append("Le titre (champ obligatoire) est vide. # ");
                    }
                    if (Util.isEmpty(values[5])) { // cote de périodique
                        lineLog.append("La cote de périodique (champ obligatoire) est vide. # ");
                    }
                    if (Util.isEmpty(values[6])) { // dates extrêmes de la collection
                        lineLog.append("Les dates extrêmes de la collection (champ obligatoire) sont vides. # ");
                    }
                    if (Util.notEmpty(values[8]) ? !contentCheck(values[8],FileDocument.class) : false) { // Inventaire de la collection
                        lineLog.append("Le champ Inventaire de la collection présente un contenu incorrect (il n'existe pas ou ne pointe pas sur un document) # ");
                    }
                    if (Util.notEmpty(values[9]) ? !categoryCheck(values[9], channel.getCategory("$jcmsplugin.archives.category.periodique.frequenceparution")) : false) { // Fréquence parution
                        lineLog.append("Le champ Fréquence parution présente une catégorie incorrecte (mauvaise branche ou ID n'existant pas) # ");
                    }
                    if (Util.notEmpty(values[10]) ? !contentCheck(values[10], Periodique.class) : false) { // Ancien titre
                        lineLog.append("Le champ Ancien titre présente des contenus incorrects (un ou plusieurs contenus n'existent pas / ne sont pas des Périodiques) # ");
                    }
                    if (Util.notEmpty(values[11]) ? !contentCheck(values[11], Periodique.class) : false) { // Titre devenu
                        lineLog.append("Le champ Titre devenu présente des contenus incorrects (un ou plusieurs contenus n'existent pas / ne sont pas des Périodiques) # ");
                    }
                    if (Util.notEmpty(values[13]) ? !contentCheck(values[13],City.class) : false) { // Siège du journal
                        lineLog.append("Le champ Siège du journal présente un contenu incorrect (il n'existe pas ou ne pointe pas sur une commune) # ");
                    }
                    if (Util.notEmpty(values[15]) ? !categoryCheck(values[15], channel.getCategory("$jcmsplugin.archives.category.periodique.theme")) : false) { // Thème
                        lineLog.append("Le champ Thème présente une ou des catégories incorrectes (mauvaise branche ou IDs n'existant pas) # ");
                    }
                    if (Util.notEmpty(values[16]) ? !categoryCheck(values[16], channel.getCategory("$jcmsplugin.archives.category.periodique.tendancespolitiques")) : false) { // Tendances politiques
                        lineLog.append("Le champ Tendances politiques présente une ou des catégories incorrectes (mauvaise branche ou IDs n'existant pas) # ");
                    }
                    if (Util.notEmpty(values[17]) ? !categoryCheck(values[17], channel.getCategory("$jcmsplugin.archives.category.periodique.periode")) : false) { // Période
                        lineLog.append("Le champ Période présente une ou des catégories incorrectes (mauvaise branche ou IDs n'existant pas) # ");
                    }
                    if (Util.notEmpty(values[18]) ? !contentCheck(values[18], City.class) : false) { // Communes concernées
                        lineLog.append("Le champ Communes concernées présente des contenus incorrects (un ou plusieurs contenus n'existent pas / ne sont pas des Communes) # ");
                    }
                    
                    Periodique itPeriodique = generatePeriodiqueFromCsvValues(values);
                    
                    ControllerStatus status = itPeriodique.checkCreate(channel.getCurrentLoggedMember());
                    if (!status.isOK()) {
                        lineLog.append(status.getMessage(channel.getCurrentUserLang()));   
                    }
                    
                    if (Util.notEmpty(lineLog.toString())) {
                        returnedLog.put("Ligne " + (cpt), lineLog.toString());
                    }
                    
                    cpt++;
                }
            } catch (IOException e) {
                channel.getCurrentJcmsContext().addMsgSession(new JcmsMessage(JcmsMessage.Level.ERROR, "Erreur lors de la lecture du CSV : " + e.getMessage()));
                return null;
            }
            
            return returnedLog;
        }
        
        channel.getCurrentJcmsContext().addMsgSession(new JcmsMessage(JcmsMessage.Level.SUCCESS, "Le fichier CSV est correct !"));
        
        return null;
    }

    /**
     * Vérifie si une liste d'IDs de contenus représente une liste correcte de contenus
     * Incorrect si un contenu n'existe pas ou n'est pas du type indiqué
     * @param string
     * @param type
     * @return
     */
    private static boolean contentCheck(String idsStr, Class<?> itClass) {
        if (Util.isEmpty(idsStr)) return false;
        
        String[] ids = idsStr.split(doubleHashtag);
        for (int counter = 0; counter < ids.length; counter++) {
            Publication itPub = channel.getPublication(ids[counter]);
            if (Util.isEmpty(itPub) || !itClass.isInstance(itPub)) return false;
        }
        return true;
    }

    /**
     * Vérifie si une liste d'IDs de catégorie représente une liste correcte de catégories
     * Incorrect si une catégorie n'existe pas ou si la branche d'une catégorie est incorrecte
     * @param string
     * @return
     */
    private static boolean categoryCheck(String cidsStr, Category ancestorCat) {
        if (Util.isEmpty(cidsStr)) return false;
        
        String[] cids = cidsStr.split(doubleHashtag);
        for (int counter = 0; counter < cids.length; counter++) {
            Category itCat = channel.getCategory(cids[counter]);
            if (Util.isEmpty(itCat) || !SocleUtils.catHasAncestor(itCat, ancestorCat)) return false;
        }
        return true;
    }

    private static boolean fileIsNotCorrectCsv(FileItem fileItem) {
        boolean result = false;
        String message = "";
        String trace = "";

        try {
            csvReader = new CsvReader(new InputStreamReader(fileItem.getInputStream(),encoding), SEPARATOR);
            String[] values;
            int cpt = 1;
            // On vérifie le nombre de colonnes
            while (csvReader.readRecord()){
                values = csvReader.getValues();
                if(values.length != 20){
                    trace += "Le fichier contient une ligne incorrecte [ligne " + cpt + "], nombre de colonnes différent de 26 \r\n" + csvReader.getRawRecord() + "\r\n";
                    result = true;
                }
                cpt++;
            }
            csvReader.close();
            if(result){
                message = "Le fichier contient des lignes en erreur.";
                channel.getCurrentJcmsContext().addMsgSession(new JcmsMessage(JcmsMessage.Level.ERROR, message));
            }
            else{
                result = false;
                message = "La syntaxe du fichier est correcte.";
                channel.getCurrentJcmsContext().addMsgSession(new JcmsMessage(JcmsMessage.Level.INFO, message));
            }
            
            channel.getCurrentJcmsContext().getRequest().setAttribute("traceImport", trace);
            
        } catch (FileNotFoundException e) {
            result = true;
            message = "Impossible de trouver le fichier";
            channel.getCurrentJcmsContext().addMsgSession(new JcmsMessage(JcmsMessage.Level.ERROR, message));
        } catch (IOException e) {
            result = true;
            message = "Erreur lors de la lecture du fichier";
            channel.getCurrentJcmsContext().addMsgSession(new JcmsMessage(JcmsMessage.Level.ERROR, message));
            channel.getCurrentJcmsContext().getRequest().setAttribute("traceImport", e);
        } 
        return result;
    }
    
    /**
     * Générer une fiche lieu à partir des valeurs d'une ligne CSV
     * @param values
     */
    private static Periodique generatePeriodiqueFromCsvValues(String[] values) {
        
        Periodique itPeriodique = new Periodique();
        // Titre
        itPeriodique.setTitle(values[0]);
        /*
        * 18 Communes concernées - City[]
        * 19 Toutes les communes du département - boolean oui/non
        */
        
        // Image bandeau
        if (Util.notEmpty(values[1])) {
            itPeriodique.setImageBandeau(values[1]);
        }
        // Copyright
        if (Util.notEmpty(values[2])) {
            itPeriodique.setCopyright(values[2]);
        }
        // Légende
        if (Util.notEmpty(values[3])) {
            itPeriodique.setLegende(values[3]);
        }
        // Texte alternatif
        if (Util.notEmpty(values[4])) {
            itPeriodique.setTexteAlternatif(values[4]);
        }
        // Cote de périodique
        itPeriodique.setCoteDePeriodique(values[5]);
        // Dates extrêmes de la collection 
        itPeriodique.setDatesExtremesDeLaCollection(values[6]);
        // Dates extrêmes de la collection numérisée
        if (Util.notEmpty(values[7])) {
            itPeriodique.setTexteAlternatif(values[7]);
        }
        // Inventaire de la collection
        if (Util.notEmpty(values[8])) {
            itPeriodique.setInventaireDeLaCollection(channel.getData(FileDocument.class, values[8]));
        }
        // Fréquence parution
        itPeriodique.addCategory(channel.getCategory(values[9]));
        // Ancien titre
        if (Util.notEmpty(values[10])) {
            List<Periodique> periodiques = new ArrayList<>();
            for (String itId : values[10].split(doubleHashtag)) {
                periodiques.add(channel.getData(Periodique.class, itId));
            }
            itPeriodique.setAncienTitre(periodiques.toArray(new Periodique[periodiques.size()]));
        }
        // Titre devenu
        if (Util.notEmpty(values[11])) {
            List<Periodique> periodiques = new ArrayList<>();
            for (String itId : values[11].split(doubleHashtag)) {
                periodiques.add(channel.getData(Periodique.class, itId));
            }
            itPeriodique.setTitreDevenu(periodiques.toArray(new Periodique[periodiques.size()]));
        }
        // Notes
        if (Util.notEmpty(values[12])) {
            itPeriodique.setNotes(values[12]);
        }
        // Siège
        if (Util.notEmpty(values[13])) {
            itPeriodique.setSiegeDuJournal(channel.getData(City.class, values[13]));
        }
        // Numérisé
        itPeriodique.setNumerise("oui".equalsIgnoreCase(values[14]));
        // Thème
        for (String itCid : values[15].split(doubleHashtag)) {
            itPeriodique.addCategory(channel.getCategory(itCid));
        }
        // Tendances politiques
        if (Util.notEmpty(values[16])) {
            for (String itCid : values[16].split(doubleHashtag)) {
                itPeriodique.addCategory(channel.getCategory(itCid));
            }
        }
        // Période
        for (String itCid : values[17].split(doubleHashtag)) {
            itPeriodique.addCategory(channel.getCategory(itCid));
        }
        // Communes concernées
        if (Util.notEmpty(values[18])) {
            List<City> communes = new ArrayList<>();
            for (String itId : values[18].split(doubleHashtag)) {
                communes.add(channel.getData(City.class, itId));
            }
            itPeriodique.setCommunesConcernees(communes.toArray(new City[communes.size()]));
        }
        // Toutes les communes
        itPeriodique.setToutesLesCommunesDuDepartement("oui".equalsIgnoreCase(values[19]));
        
        itPeriodique.setAuthor(channel.getCurrentLoggedMember());
        
        return itPeriodique;
    }
    
    /**
     * Procéder à l'import de fiches lieu via CSV
     * Assume que le fichier a été vérifié au préalable
     * @param fileItem
     */
    public static void importPeriodiquesCsv(FileItem fileItem) {
        
        List<Periodique> periodiquesToCreate = new ArrayList<Periodique>();
        
        try {
            csvReader = new CsvReader(new InputStreamReader(fileItem.getInputStream(),encoding), SEPARATOR);
            String[] values;
            boolean skippedFirst = false;
            while (csvReader.readRecord()){
                if (!skippedFirst) { // ne pas lire la ligne des headers
                    skippedFirst = true;
                    continue;
                }

                values = csvReader.getValues();
                
                periodiquesToCreate.add(generatePeriodiqueFromCsvValues(values));
            }
            
        } catch (Exception e) {
            channel.getCurrentJcmsContext().addMsgSession(new JcmsMessage(JcmsMessage.Level.ERROR, "Erreur lors de la lecture du CSV : " + e.getMessage()));
            LOGGER.error("Erreur sur importFichesLieuCsv : " + e.getMessage());
        }
        
        int counterImported = 0;
        
        for (Periodique itPeriodique : periodiquesToCreate) {
            ControllerStatus importStatus = itPeriodique.checkAndPerformCreate(channel.getCurrentLoggedMember());
            if (!importStatus.isOK()) {
                LOGGER.warn("Anomalie lors de l'import du lieu " + itPeriodique.getTitle() + " : " + importStatus.getMessage(channel.getCurrentUserLang()));
            } else {
                counterImported++;
            }
        }
        
        if (counterImported == periodiquesToCreate.size()) {        
            channel.getCurrentJcmsContext().addMsgSession(new JcmsMessage(JcmsMessage.Level.SUCCESS, "Un total de " + periodiquesToCreate.size() + " fiches lieux ont été créées."));
        } else {
            channel.getCurrentJcmsContext().addMsgSession(new JcmsMessage(JcmsMessage.Level.WARN, "Un total de " + counterImported
                + " fiches lieux ont été créées sur les " + periodiquesToCreate.size() + " possibles. Veuillez inspecter les logs.")); 
        }
    }
}