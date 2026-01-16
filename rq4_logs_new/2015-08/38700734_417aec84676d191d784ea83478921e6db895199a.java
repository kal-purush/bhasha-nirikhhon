package it.algos.webbase.prova;

import it.algos.webbase.web.ABootStrap;
import it.algos.webbase.web.entity.EM;
import it.algos.webbase.domain.versione.LibVers;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

/**
 * Executed on container startup
 * <p>
 * Setup non-UI logic here <br>
 * Classe eseguita solo quando l'applicazione viene caricata/parte nel server (Tomcat od altri) <br>
 * Eseguita quindi ad ogni avvio/riavvio del server e NON ad ogni sessione <br>
 * È OBBLIGATORIO creare la sottoclasse per regolare il valore della persistence unit che crea l'EntityManager <br>
 */
public class ProvaABootStrap extends ABootStrap {

    /**
     * Valore standard suggerito per ogni progetto
     * Questo singolo progetto può modificarlo nel metodo setPersistenceEntity()
     */
    private static final String DEFAULT_PERSISTENCE_UNIT = EM.DEFAULT_PERSISTENCE_UNIT;

    /**
     * Regola il valore della persistence unit per creare l'EntityManager <br>
     * Deve essere sovrascritto (obbligatorio) nella sottoclasse del progetto <br>
     * Se si modifica qui, occorre sincronizzare il file persistence.xml <br>
     */
    @Override
    public void setPersistenceEntity() {
        EM.PERSISTENCE_UNIT = DEFAULT_PERSISTENCE_UNIT;
    }// end of method


    /**
     * Executed on container startup
     * <p>
     * Setup non-UI logic here
     */
    @Override
    public void contextInitialized(ServletContextEvent sce) {
        super.contextInitialized(sce);
        ServletContext svltCtx = ABootStrap.getServletContext();

        // Do server stuff here
    }// end of method


    /**
     * Tear down logic here<br>
     * Sovrascritto
     */
    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        super.contextDestroyed(sce);
    }// end of method

    /**
     * tutte le aggiunte, modifiche e patch vengono inserite con una versione<br>
     * l'ordine di inserimento è FONDAMENTALE
     * <p>
     * Se le versioni aumentano, conviene spostare in una classe esterna
     */
    private void versioneBootStrap(ServletContext svltCtx) {
        //--prima installazione del programma
        //--non fa nulla, solo informativo
        if (LibVers.installa(1)) {
            LibVers.nuova("Setup", "Installazione iniziale");
        }// fine del blocco if
    }// end of method

}// end of boot class