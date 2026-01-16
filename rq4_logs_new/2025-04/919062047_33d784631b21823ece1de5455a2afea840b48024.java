package telegram.files.maintains;


import cn.hutool.core.collection.IterUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.templates.SqlTemplate;
import org.drinkless.tdlib.TdApi;
import telegram.files.*;
import telegram.files.repository.FileRecord;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This verticle is responsible for maintaining the thumbnails of the media files.
 */
public class ThumbnailMaintainVerticle extends MaintainVerticle {

    // thumbnail unique id -> file_record unique id
    private final Map<String, String> downloadingThumbnailUniqueIds = new ConcurrentHashMap<>();

    private volatile int count = 0;

    private volatile boolean scanning = true;

    private volatile int lastDownloadingCount = 0;

    private volatile long lastTime = 0;

    @Override
    public void start(Promise<Void> startPromise) {
        initEventConsumer();
        super.start(startPromise, this::handleThumbnail);
        vertx.setPeriodic(5000, id -> this.waitForThumbnailDownload());
    }

    private void handleThumbnail() {
        timeInterval.start();
        log.info("🔨 Start to handle thumbnail");
        try {
            log.debug("🔨 1.Scan all file records missing thumbnail and download them");
            long fromMessageId = 0;
            long page = 1;
            while (true) {
                log.debug("🔨 Scan page %d, limit 100".formatted(page));
                List<FileRecord> rows = Future.await(SqlTemplate.forQuery(DataVerticle.pool, """
                                SELECT * FROM file_record
                                WHERE thumbnail_unique_id is null AND type != 'thumbnail'
                                %s
                                ORDER BY message_id desc LIMIT 100
                                """.formatted(fromMessageId == 0 ? "" : " AND message_id < #{fromMessageId}")
                        )
                        .mapTo(FileRecord.ROW_MAPPER)
                        .execute(MapUtil.of("fromMessageId", fromMessageId))
                        .map(IterUtil::toList));

                if (rows.isEmpty()) {
                    log.debug("🔨 No more file records found, scan finished");
                    break;
                }

                for (FileRecord fileRecord : rows) {
                    if (downloadThumbnail(fileRecord)) {
                        count++;
                    }
                }

                fromMessageId = rows.getLast().messageId();
                page++;
            }
            log.info("✅ All %d file records with missing thumbnail start to download".formatted(count));
            scanning = false;
        } catch (Exception e) {
            log.error("🔨 Failed to handle thumbnail", e);
            super.end(false, e);
        }
    }

    private void waitForThumbnailDownload() {
        if (!scanning && downloadingThumbnailUniqueIds.isEmpty()) {
            log.info("✅ All thumbnails downloaded successfully, finished");
            super.end(true, null);
            return;
        }
        log.info("🔨 Processing %d thumbnails of %d. Time consumed: %s".formatted(downloadingThumbnailUniqueIds.size(),
                count,
                timeInterval.intervalPretty()));
        if (!scanning && downloadingThumbnailUniqueIds.size() == lastDownloadingCount
            && System.currentTimeMillis() - lastTime > 60 * 1000) {
            log.error("🔨 Thumbnail download timeout, %d thumbnails are still downloading".formatted(downloadingThumbnailUniqueIds.size()));
            super.end(false, new RuntimeException("Thumbnail download timeout"));
        }
        if (!scanning && downloadingThumbnailUniqueIds.size() < lastDownloadingCount) {
            lastDownloadingCount = downloadingThumbnailUniqueIds.size();
            lastTime = System.currentTimeMillis();
        }
    }

    private void initEventConsumer() {
        vertx.eventBus().consumer(EventEnum.TELEGRAM_EVENT.address(), message -> {
            JsonObject jsonObject = (JsonObject) message.body();
            EventPayload payload = jsonObject.getJsonObject("payload").mapTo(EventPayload.class);
            if (payload == null || payload.type() != EventPayload.TYPE_FILE_STATUS) {
                return;
            }

            if (payload.data() != null && payload.data() instanceof Map<?, ?> data && StrUtil.isNotBlank((String) data.get("downloadStatus"))) {
                FileRecord.DownloadStatus downloadStatus = FileRecord.DownloadStatus.valueOf((String) data.get("downloadStatus"));
                if (downloadStatus != FileRecord.DownloadStatus.completed) {
                    return;
                }
                String thumbnailUniqueId = (String) data.get("uniqueId");
                if (downloadingThumbnailUniqueIds.containsKey(thumbnailUniqueId)
                    && updateThumbnailUniqueId(downloadingThumbnailUniqueIds.get(thumbnailUniqueId), thumbnailUniqueId)) {
                    downloadingThumbnailUniqueIds.remove(thumbnailUniqueId);
                }
            }
        });
    }

    private boolean downloadThumbnail(FileRecord fileRecord) {
        try {
            Optional<TelegramVerticle> telegramVerticleOptional = TelegramVerticles.get(fileRecord.telegramId());
            if (telegramVerticleOptional.isEmpty()) {
                log.error("🔨 Telegram verticle not found for telegram id: %d".formatted(fileRecord.telegramId()));
                return false;
            }
            TelegramVerticle telegramVerticle = telegramVerticleOptional.get();
            TdApi.Message message = Future.await(telegramVerticle.client.execute(new TdApi.GetMessage(fileRecord.chatId(), fileRecord.messageId())));
            Optional<FileRecord> thumbnailRecordOptional = TdApiHelp.getFileHandler(message)
                    .map(fileHandler -> fileHandler.convertThumbnailRecord(telegramVerticle.telegramRecord.id()));
            if (thumbnailRecordOptional.isEmpty()) {
                return false;
            }
            FileRecord thumbnailRecord = thumbnailRecordOptional.get();
            downloadingThumbnailUniqueIds.put(thumbnailRecord.uniqueId(), fileRecord.uniqueId());
            Boolean download = Future.await(telegramVerticle.downloadThumbnail(fileRecord.chatId(),
                    fileRecord.messageId(),
                    thumbnailRecord
            ));
            if (!download) {
                downloadingThumbnailUniqueIds.remove(thumbnailRecord.uniqueId());
                if (thumbnailRecord.isDownloadStatus(FileRecord.DownloadStatus.completed)) {
                    updateThumbnailUniqueId(fileRecord.uniqueId(), thumbnailRecord.uniqueId());
                }
                return false;
            }
            return true;
        } catch (Exception e) {
            log.error(e, "🔨 Failed to download thumbnail, file unique id: %s".formatted(fileRecord.uniqueId()));
            return false;
        }
    }

    private boolean updateThumbnailUniqueId(String uniqueId, String thumbnailUniqueId) {
        return Future.await(SqlTemplate.forUpdate(DataVerticle.pool, """
                        UPDATE file_record
                        SET thumbnail_unique_id = #{thumbnailUniqueId}
                        WHERE unique_id = #{uniqueId}
                        """)
                .execute(MapUtil.ofEntries(
                        MapUtil.entry("uniqueId", uniqueId),
                        MapUtil.entry("thumbnailUniqueId", thumbnailUniqueId)
                ))
                .onFailure(err -> log.error(err, "🔨 Failed to update thumbnail unique id: %s".formatted(uniqueId)))
                .map(true));
    }
}