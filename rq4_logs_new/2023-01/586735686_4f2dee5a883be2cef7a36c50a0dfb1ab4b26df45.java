package tososomaru.wb.ads.rest.campaigns;

import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tososomaru.wb.ads.campaigns.Campaign;
import tososomaru.wb.ads.usecase.campaigns.*;

import java.util.List;
import java.util.UUID;

@AllArgsConstructor
@RestController
@RequestMapping(path = "/campaigns")
@Tag(name = "Рекламные кампании", description = "API рекламных кампаний")
public class CampaignsController {
    private final CreateCampaign createCampaign;
    private final DeleteCampaign deleteCampaign;
    private final ResumeCampaign resumeCampaign;
    private final SuspendCampaign suspendCampaign;
    private final SynchronizeCampaigns synchronizeCampaigns;
    private final UpdateCampaign updateCampaign;
    private final GetCampaignById getCampaignById;
    private final GetAllCampaigns getAllCampaigns;

    /**
     * Получить кампанию по ID
     * @param id ID кампании
     * @return Рекламная кампания {@link Campaign}
     */
    @GetMapping(path = "/{id}")
    public ResponseEntity<Campaign> getCampaign(@PathVariable String id) {
        return ResponseEntity.ok(
                getCampaignById.execute(UUID.fromString(id))
        );
    }

    /**
     * Получить все рекламные кампании
     * @return List<Campaign
     */
    @GetMapping
    public ResponseEntity<List<Campaign>> getAllCampaign() {
        return ResponseEntity.ok(
                getAllCampaigns.execute()
        );
    }

    /**
     * Создать рекламную кампанию
     * @param request Полезная нагрузка
     * @return {@link Campaign}
     */
    @PostMapping
    public ResponseEntity<Campaign> createCampaign(
            @RequestBody CreateCampaign.CreateCampaignRequest request
    ) {
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(
                      createCampaign.execute(request)
                );
    }

    /**
     * Удалить кампанию по ее идентификатору
     * @param id ID капмнаии
     */
    @DeleteMapping(path = "/{id}")
    public ResponseEntity<Void> deleteCampaign(
            @PathVariable String id
    ) {
        deleteCampaign.execute(id);
        return ResponseEntity.noContent().build();
    }

    /**
     * Приостановить рекламную кампанию
     * @param id ID кампании
     */
    @PatchMapping(path = "/{id}/suspend")
    public ResponseEntity<Void> suspendCampaign(
            @PathVariable String id
    ) {
        suspendCampaign.execute(id);
        return ResponseEntity.accepted().build();
    }

    /**
     * Возобновить рекламную кампанию
     * @param id ID кампании
     */
    @PatchMapping(path = "/{id}/resume")
    public ResponseEntity<Void> resumeCampaign(
            @PathVariable String id
    ) {
        resumeCampaign.execute(id);
        return ResponseEntity.accepted().build();
    }

    /**
     * Синхронизировать все рекламные кампании сервиса с кампаниями WB
     * @return {@link List<Campaign>}
     */
    @PatchMapping(path = "/synchronize")
    public ResponseEntity<List<Campaign>> synchronizeCampaign() {
        return ResponseEntity.accepted()
                .body(synchronizeCampaigns.execute());
    }

    /**
     * Обновить параметры рекламной кампании
     * @param id ID рекламной кампании
     * @param request Полезная нагрузка
     * @return {@link Campaign}
     */
    @PatchMapping(path = "/{id}")
    public ResponseEntity<Campaign> updateCampaign(
            @PathVariable String id,
            @RequestBody UpdateCampaign.UpdateCampaignRequest request
            ) {
        return ResponseEntity.accepted()
                .body(
                        updateCampaign.execute(request)
                );
    }
}