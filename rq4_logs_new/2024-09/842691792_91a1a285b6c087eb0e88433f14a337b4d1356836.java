package techclallenge5.fiap.com.msGestaoItem.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import techclallenge5.fiap.com.msGestaoItem.model.Item;
import techclallenge5.fiap.com.msGestaoItem.service.ItemServiceImpl;
import techclallenge5.fiap.com.msGestaoItem.utils.ItemHelper;

class ItemControllerTest {

    @InjectMocks
    private ItemController itemController;
    @Mock
    private ItemServiceImpl itemServiceImplMock;
    AutoCloseable openMocks;

    private final Item itemMock = ItemHelper.gerarItem();
    private final Item itemAtualizacaoMock = ItemHelper.gerarItemAtualizacao();

    @BeforeEach
    public void setUp() {
        openMocks = MockitoAnnotations.openMocks(this);
        BDDMockito.when(itemServiceImplMock.buscarItens())
                .thenReturn(Flux.just(itemMock));

        BDDMockito.when(itemServiceImplMock.buscarItemPeloID(ArgumentMatchers.anyString()))
                .thenReturn(Mono.just(itemMock));

        BDDMockito.when(itemServiceImplMock.criarItem(ItemHelper.gerarItem()))
                .thenReturn(Mono.just(itemMock));

        BDDMockito.when(itemServiceImplMock.atualizarItem(ItemHelper.gerarItemAtualizacao()))
                .thenReturn(Mono.just(itemAtualizacaoMock));

        BDDMockito.when(itemServiceImplMock.deleteItem("1"))
                .thenReturn(Mono.empty());

    }

    @Test
    public void deveBuscarItensComSucesso() {
        StepVerifier.create(itemController.buscarItens())
                .expectSubscription()
                .expectNext(itemMock)
                .verifyComplete();
    }

    @Test
    public void devebuscarItemPeloIDComSucesso() {
        StepVerifier.create(itemController.buscarItemPeloID(itemMock.getId()))
                .expectSubscription()
                .expectNext(itemMock)
                .verifyComplete();
    }

    @Test
    public void deveCriarItemComSucesso() {
        var item = ItemHelper.gerarItem();

        StepVerifier.create(itemController.criarItem(item))
                .expectSubscription()
                .expectNext(itemMock)
                .verifyComplete();
    }

    @Test
    public void deveAtualizarItemComSucesso() {
        var item = ItemHelper.gerarItemAtualizacao();

        StepVerifier.create(itemController.atualizarItem(item))
                .expectSubscription()
                .expectNext(itemAtualizacaoMock)
                .verifyComplete();
    }

    @Test
    public void deveDeletarItemComSucesso() {
        StepVerifier.create(itemController.deletarItem(itemMock.getId()))
                .expectSubscription()
                .verifyComplete();
    }

}