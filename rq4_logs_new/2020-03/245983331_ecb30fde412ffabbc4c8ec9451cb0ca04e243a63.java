package com.ipiecoles.java.java350.service;

import com.ipiecoles.java.java350.exception.EmployeException;
import com.ipiecoles.java.java350.model.Employe;
import com.ipiecoles.java.java350.model.NiveauEtude;
import com.ipiecoles.java.java350.model.Poste;
import com.ipiecoles.java.java350.repository.EmployeRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@ExtendWith(MockitoExtension.class)
public class EmployeServiceTest {

    @InjectMocks
    private EmployeService employeService;
    @Mock
    private EmployeRepository employeRepository;

    @ParameterizedTest(name = "Le {2} {1} {0} de niveau {3} de matricule {6} " +
            "a été embauché à temps plein pour un salaire de {7} €")
    @CsvSource({
            "'Doe', 'John', 'COMMERCIAL', 'BTS_IUT', 1.0, , 'C00001', 1825.46",
            "'Doe', 'Johnny', 'COMMERCIAL', 'BTS_IUT', 1.0, '12345', 'C12346', 1825.46",
            "'Doy', 'John', 'TECHNICIEN', 'BTS_IUT', 1.0, '23456', 'T23457', 1825.46",
    })
    public void testEmbaucheEmployeTempsPlein(
            String nom, String prenom, Poste poste, NiveauEtude niveauEtude, Double tempsPartiel,
            String lastMatricule, String matricule, Double salaire) throws EmployeException {
        //Given
        Mockito.when(employeRepository.findLastMatricule()).thenReturn(lastMatricule);
        Mockito.when(employeRepository.findByMatricule(matricule)).thenReturn(null);

        //When
        employeService.embaucheEmploye(nom, prenom, poste, niveauEtude, tempsPartiel);

        //Then
        ArgumentCaptor<Employe> employeCaptor = ArgumentCaptor.forClass(Employe.class);
        Mockito.verify(employeRepository, Mockito.times(1)).save(employeCaptor.capture());
        Employe employe = employeCaptor.getValue();
        Assertions.assertThat(employe.getNom()).isEqualTo(nom);
        Assertions.assertThat(employe.getPrenom()).isEqualTo(prenom);
        Assertions.assertThat(employe.getMatricule()).isEqualTo(matricule);
        Assertions.assertThat(employe.getTempsPartiel()).isEqualTo(tempsPartiel);
        //Si dateEmbauche est au millième de secondes.
        Assertions.assertThat(
                employe.getDateEmbauche().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
        ).isEqualTo(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")));
        Assertions.assertThat(employe.getSalaire()).isEqualTo(salaire);
        //Performance_base = 1
        Assertions.assertThat(employe.getPerformance()).isEqualTo(1);
    }

    @Test
    public void testEmbaucheEmployeLimiteMatricule() {
        //Given
        String nom = "Doe";
        String prenom ="John";
        Poste poste = Poste.COMMERCIAL;
        NiveauEtude niveauEtude = NiveauEtude.BTS_IUT;
        Double tempsPartiel = 1.0;
        Mockito.when(employeRepository.findLastMatricule()).thenReturn("99999");
        Mockito.when(employeRepository.findByMatricule("100000")).thenReturn(null);

        //When & Then
        Assertions.assertThatThrownBy(() -> {
            employeService.embaucheEmploye(nom, prenom, poste, niveauEtude, tempsPartiel);
        }).isInstanceOf(EmployeException.class).hasMessage("Limite des 100000 matricules atteinte !");

        /* plus basique
        try {
            employeService.embaucheEmploye(nom, prenom, poste, niveauEtude, tempsPartiel);
            // Si aucune exception est levée
            Assertions.fail("Aurait dû planter !");
        } catch (Exception e) {
            Assertions.assertThat(e).isInstanceOf(EmployeException.class);
            Assertions.assertThat(e.getMessage()).isEqualTo("Limite des 100000 matricules atteinte !");
        }
        */
    }
}