package com.dummy.myerp.testbusiness.business.manager;

import com.dummy.myerp.business.impl.TransactionManager;
import com.dummy.myerp.business.impl.manager.ComptabiliteManagerImpl;
import com.dummy.myerp.consumer.dao.impl.db.dao.ComptabiliteDaoImpl;
import com.dummy.myerp.model.bean.comptabilite.CompteComptable;
import com.dummy.myerp.model.bean.comptabilite.EcritureComptable;
import com.dummy.myerp.model.bean.comptabilite.JournalComptable;
import com.dummy.myerp.model.bean.comptabilite.LigneEcritureComptable;
import com.dummy.myerp.technical.exception.FunctionalException;
import com.dummy.myerp.technical.exception.NotFoundException;
import com.dummy.myerp.testbusiness.business.BusinessTestCase;
import com.dummy.myerp.testbusiness.business.SpringRegistry;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;


/**
 * Classe de test de l'initialisation du contexte Spring
 */
public class CompatibiliteManagerImplTest extends BusinessTestCase {

    private ComptabiliteManagerImpl manager = new ComptabiliteManagerImpl();
    private ComptabiliteDaoImpl dao = ComptabiliteDaoImpl.getInstance();
    private TransactionManager trManager = TransactionManager.getInstance();
    private static SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy");


    /**
     * Constructeur.
     */
    public CompatibiliteManagerImplTest() {
        super();
    }


    /**
     * Teste l'initialisation du contexte Spring
     */
    @Before
    public void testInit() {
        SpringRegistry.init();
        assertNotNull(SpringRegistry.getBusinessProxy());
        assertNotNull(SpringRegistry.getTransactionManager());
    }

    @Test
    public void insertEcritureComptable() throws FunctionalException, ParseException, NotFoundException {

        EcritureComptable vEcriture = new EcritureComptable();

        //GIVEN

        vEcriture.setDate(SIMPLE_DATE_FORMAT.parse("01/01/2020"));
        vEcriture.setLibelle("libelle");
        vEcriture.setJournal(new JournalComptable("AC", "Achat"));
        vEcriture.getListLigneEcriture().add(new LigneEcritureComptable(new CompteComptable(401),
                null, null,
                new BigDecimal(123)));
        vEcriture.getListLigneEcriture().add(new LigneEcritureComptable(new CompteComptable(411),
                null, new BigDecimal(123),
                null));

        //WHEN

        manager.addReference(vEcriture);

        manager.insertEcritureComptable(vEcriture);

        // THEN

        EcritureComptable vERef = dao.getEcritureComptableByRef(vEcriture.getReference());

        assertThat(vEcriture.getDate()).isEqualTo(vERef.getDate());
        assertThat(vEcriture.getReference()).isEqualTo(vERef.getReference());
        assertThat(vEcriture.getId()).isEqualTo(vERef.getId());


    }

    @Test
    public void updateEcritureComptable() throws FunctionalException, NotFoundException, ParseException {

        //GIVEN
        EcritureComptable vERef = dao.getEcritureComptable(-3);
        String nouveauLibelle = "Libéllé ttt du " + new Date();
        vERef.setLibelle(nouveauLibelle);

        //WHEN

        manager.addReference(vERef);
        manager.updateEcritureComptable(vERef);

        //THEN
        String vELib = dao.getEcritureComptable(-3).getLibelle();

        assertThat(vERef.getLibelle()).isEqualTo(vELib);

    }

    @Test(expected = NotFoundException.class)
    public void deleteEcritureComptable() throws FunctionalException, ParseException, NotFoundException {
        // GIVEN
        EcritureComptable vEcriture = new EcritureComptable();

        //GIVEN

        vEcriture.setDate(SIMPLE_DATE_FORMAT.parse("01/01/2020"));
        vEcriture.setLibelle("Pour test a détruire");
        vEcriture.setJournal(new JournalComptable("AC", "Achat"));
        vEcriture.getListLigneEcriture().add(new LigneEcritureComptable(new CompteComptable(401),
                null, null,
                new BigDecimal(123)));
        vEcriture.getListLigneEcriture().add(new LigneEcritureComptable(new CompteComptable(411),
                null, new BigDecimal(123),
                null));


        manager.addReference(vEcriture);

        manager.insertEcritureComptable(vEcriture);

        //WHEN
        manager.deleteEcritureComptable(vEcriture.getId());

        assertThat(dao.getEcritureComptable(vEcriture.getId())).withFailMessage(String.valueOf(new NotFoundException()));

    }

}