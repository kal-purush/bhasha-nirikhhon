package com.bearAndPupperCo.sangenWrestlingApp.Repository;

import com.bearAndPupperCo.sangenWrestlingApp.DTO.WrestlerMainTableDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@ExtendWith(SpringExtension.class)
@SpringBootTest
public class WrestlerRepoImplTest {

    @Autowired
    private WrestlerRepoImpl wrestlerRepo;

    private int page = 1;
    private int size = 10;
    private Integer brandId = 1;
    private Integer lockerId = 1;
    private String orderBy = "in_ring_name";
    private String orderDirection = "ASC";

    @Test
    public void findWrestlerListByRingNameASC() {
        brandId = null;
        lockerId = null;

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Aaron Schultz");

    }

    @Test
    public void findWrestlerListByRingNameDESC() {
        brandId = null;
        lockerId = null;
        orderDirection = "DESC";

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Yuri Matsumoto");

    }

    @Test
    public void findWrestlerListByRingNameSmackdownASC() {
        brandId = 1;
        lockerId = null;

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Dr. Deuce");

    }

    @Test
    public void findWrestlerListByRingNameSmackdownDESC() {
        brandId = 1;
        lockerId = null;
        orderDirection = "DESC";

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Tiger Tia");

    }

    @Test
    public void findWrestlerListByRingNameSangenASC() {
        brandId = 2;
        lockerId = null;

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Aaron Schultz");

    }

    @Test
    public void findWrestlerListByRingNameSangenDESC() {
        brandId = 2;
        lockerId = null;
        orderDirection = "DESC";

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Yuri Matsumoto");

    }

    @Test
    public void findWrestlerListByRingNameSmackdownWomenASC() {
        brandId = 1;
        lockerId = 1;

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Rebecca Lawrance");

    }

    @Test
    public void findWrestlerListByRingNameSmackdownWomenDESC() {
        brandId = 1;
        lockerId = 1;
        orderDirection = "DESC";

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Tiger Tia");

    }

    @Test
    public void findWrestlerListByRingNameSmackdownMenASC() {
        brandId = 1;
        lockerId = 2;

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Dr. Deuce");

    }

    @Test
    public void findWrestlerListByRingNameSmackdownMenDesc() {
        brandId = 1;
        lockerId = 2;
        orderDirection = "DESC";

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Raymound Hunter");

    }

    @Test
    public void findWrestlerListByRingNameSangenWomenASC() {
        brandId = 2;
        lockerId = 1;

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Adorable Ivy");

    }

    @Test
    public void findWrestlerListByRingNameSangenWomenDESC() {
        brandId = 2;
        lockerId = 1;
        orderDirection = "DESC";

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Rowdy Jay");

    }

    @Test
    public void findWrestlerListByRingNameSangenMenASC() {
        brandId = 2;
        lockerId = 2;

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Aaron Schultz");

    }

    @Test
    public void findWrestlerListByRingNameSangenMenDesc() {
        brandId = 2;
        lockerId = 2;
        orderDirection = "DESC";

        List<WrestlerMainTableDTO> wrestlers = wrestlerRepo
                .findWrestlerListByParams(page, size, brandId, lockerId, orderBy, orderDirection);

        assertNotNull(wrestlers, "Wrestler list should not be null");
        assertEquals(wrestlers.stream().findFirst().get().getInRingName(),"Yuri Matsumoto");

    }



    @Test
    void findTotalWrestlerCount() {
        brandId = null;
        lockerId = null;

        int totalWrestler = wrestlerRepo.getTotalWrestlerCount(brandId, lockerId);

        assertEquals(totalWrestler,20);

    }

    @Test
    void findTotalWrestlerCountSmackdown() {
        brandId = 1;
        lockerId = null;

        int totalWrestler = wrestlerRepo.getTotalWrestlerCount(brandId, lockerId);

        assertEquals(totalWrestler,5);
    }

    @Test
    void findTotalWrestlerCountSangen() {
        brandId = 1;
        lockerId = null;

        int totalWrestler = wrestlerRepo.getTotalWrestlerCount(brandId, lockerId);

        assertEquals(totalWrestler,5);
    }

    @Test
    void findTotalWrestlerCountWomen() {
        brandId = null;
        lockerId = 1;

        int totalWrestler = wrestlerRepo.getTotalWrestlerCount(brandId, lockerId);

        assertEquals(totalWrestler,7);
    }

    @Test
    void findTotalWrestlerCountMen() {
        brandId = null;
        lockerId = 2;

        int totalWrestler = wrestlerRepo.getTotalWrestlerCount(brandId, lockerId);

        assertEquals(totalWrestler,13);
    }

    @Test
    void findTotalWrestlerCountSmackdownWomen() {
        brandId = 1;
        lockerId = 1;

        int totalWrestler = wrestlerRepo.getTotalWrestlerCount(brandId, lockerId);

        assertEquals(totalWrestler,2);
    }

    @Test
    void findTotalWrestlerCountSmackdownMen() {
        brandId = 1;
        lockerId = 2;

        int totalWrestler = wrestlerRepo.getTotalWrestlerCount(brandId, lockerId);

        assertEquals(totalWrestler,3);
    }

    @Test
    void findTotalWrestlerCountSangenWomen() {
        brandId = 2;
        lockerId = 1;

        int totalWrestler = wrestlerRepo.getTotalWrestlerCount(brandId, lockerId);

        assertEquals(totalWrestler,5);
    }

    @Test
    void findTotalWrestlerCountSangenMen() {
        brandId = 2;
        lockerId = 2;

        int totalWrestler = wrestlerRepo.getTotalWrestlerCount(brandId, lockerId);

        assertEquals(totalWrestler,10);
    }

}