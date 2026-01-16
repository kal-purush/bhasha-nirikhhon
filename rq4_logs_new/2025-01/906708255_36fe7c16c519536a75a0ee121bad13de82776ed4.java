package tn.esprit.tpfoyer.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import tn.esprit.tpfoyer.entity.Bloc;
import tn.esprit.tpfoyer.repository.BlocRepository;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class BlocServiceImplTest {

    @Mock
    private BlocRepository blocRepository;

    @InjectMocks
    private BlocServiceImpl blocService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void retrieveAllBlocs() {
        List<Bloc> mockBlocs = List.of(new Bloc(), new Bloc());
        when(blocRepository.findAll()).thenReturn(mockBlocs);

        List<Bloc> blocs = blocService.retrieveAllBlocs();

        assertEquals(2, blocs.size());
        verify(blocRepository, times(1)).findAll();
    }

    @Test
    void retrieveBlocsSelonCapacite() {
        Bloc b1 = new Bloc();
        b1.setCapaciteBloc(5);
        Bloc b2 = new Bloc();
        b2.setCapaciteBloc(10);
        List<Bloc> mockBlocs = List.of(b1, b2);
        when(blocRepository.findAll()).thenReturn(mockBlocs);

        List<Bloc> result = blocService.retrieveBlocsSelonCapacite(6);

        assertEquals(1, result.size());
        assertEquals(10, result.get(0).getCapaciteBloc());
        verify(blocRepository, times(1)).findAll();
    }

    @Test
    void retrieveBloc() {
        Bloc mockBloc = new Bloc();
        when(blocRepository.findById(1L)).thenReturn(Optional.of(mockBloc));

        Bloc bloc = blocService.retrieveBloc(1L);

        assertNotNull(bloc);
        verify(blocRepository, times(1)).findById(1L);
    }

    @Test
    void addBloc() {
        Bloc mockBloc = new Bloc();
        when(blocRepository.save(mockBloc)).thenReturn(mockBloc);

        Bloc savedBloc = blocService.addBloc(mockBloc);

        assertNotNull(savedBloc);
        verify(blocRepository, times(1)).save(mockBloc);
    }

    @Test
    void modifyBloc() {
        Bloc mockBloc = new Bloc();
        when(blocRepository.save(mockBloc)).thenReturn(mockBloc);

        Bloc updatedBloc = blocService.modifyBloc(mockBloc);

        assertNotNull(updatedBloc);
        verify(blocRepository, times(1)).save(mockBloc);
    }

    @Test
    void removeBloc() {
        doNothing().when(blocRepository).deleteById(1L);

        blocService.removeBloc(1L);

        verify(blocRepository, times(1)).deleteById(1L);
    }

    @Test
    void trouverBlocsSansFoyer() {
        List<Bloc> mockBlocs = List.of(new Bloc(), new Bloc());
        when(blocRepository.findAllByFoyerIsNull()).thenReturn(mockBlocs);

        List<Bloc> blocsSansFoyer = blocService.trouverBlocsSansFoyer();

        assertEquals(2, blocsSansFoyer.size());
        verify(blocRepository, times(1)).findAllByFoyerIsNull();
    }

    @Test
    void trouverBlocsParNomEtCap() {
        Bloc b1 = new Bloc();
        b1.setNomBloc("Bloc A");
        b1.setCapaciteBloc(10);
        List<Bloc> mockBlocs = List.of(b1);
        when(blocRepository.findAllByNomBlocAndCapaciteBloc("Bloc A", 10)).thenReturn(mockBlocs);

        List<Bloc> blocs = blocService.trouverBlocsParNomEtCap("Bloc A", 10);

        assertEquals(1, blocs.size());
        assertEquals("Bloc A", blocs.get(0).getNomBloc());
        assertEquals(10, blocs.get(0).getCapaciteBloc());
        verify(blocRepository, times(1)).findAllByNomBlocAndCapaciteBloc("Bloc A", 10);
    }
}