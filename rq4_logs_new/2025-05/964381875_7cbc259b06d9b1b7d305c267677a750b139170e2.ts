import { dataSource } from '../db/data-source';
import { redis } from '../db/redis-source'

import { ShowtimesEntity } from '../entities/Showtimes';
import getLogger from '../utils/logger';

import { seatInventoryService, initSeatInventoryService } from '../utils/seatInventory';

const logger = getLogger('InitRedisScript'); // 使用您的 logger

export async function initRedisShowtimeInventory(showtimeId: string) {
    try {
        // move to seeds (獨立使用腳本要打開)
        // await dataSource.initialize();
        // await redis.ping()
        initSeatInventoryService(redis)

        const showtimeRepository = dataSource.getRepository(ShowtimesEntity);

        // 從資料庫中獲取場次及其所有區塊設定
        const showtime = await showtimeRepository.findOne({
            where: { id: showtimeId },
            relations: ['showtimeSections'], 
        });

        if (!showtime) {
            logger.info(`找不到 ID 為 ${showtimeId} 的場次資料。`);
            return;
        }

        if (!showtime.showtimeSections || showtime.showtimeSections.length === 0) {
            logger.warn(`場次 ${showtimeId} 未定義座位區塊。跳過 Redis 初始化。`);
            return;
        }

        // 整理成 initializeShowtimeSeats 所需的格式
        const zonesToInitialize = showtime.showtimeSections.map(section => ({
            section: section.section,
            capacity: section.capacity ?? 0
        }));

        await seatInventoryService.initializeActivitySeats(showtime.id, zonesToInitialize);
    } catch (error) {
        logger.error(`初始化場次 ${showtimeId} 的 Redis 庫存時發生錯誤:`, error);
    //move to seeds (獨立使用腳本要打開)
    // } finally {
    //     // 關閉資料庫連接
    //     if (dataSource.isInitialized) {
    //         await dataSource.destroy();
    //         logger.info('資料庫 DataSource 已關閉。');
    //     }

    //     // 關閉 Redis 客戶端連接
    //     redis.quit();
    //     logger.info('Redis 已斷開連接。');
    }
}

// --- 如何運行此腳本 ---
// 從命令列參數獲取 showtimeId
// const targetShowtimeId = process.argv[2];

// if (!targetShowtimeId) {
//     logger.error('使用方法：npx ts-node .\src\scripts\init-redis-inventory.ts <showtime_id_uuid>');
//     process.exit(1); // 退出並表示錯誤
// }

// // 執行初始化函數
// initRedisShowtimeInventory(targetShowtimeId);

// check docker redis transaction, (command list)
// bash> docker ps
//// from the previous command, found the container ID of 'redis_service'
// bash> docker exec -it <redis container id> redis-cli
// 127.0.0.1:xxxx> KEYS *
//// pickup the observed KEY from the list
// 127.0.0.1:xxxx> HGETALL <your pickup key>
// eg. HGETALL showtime_seats:11111111-1111-1111-4444-111111111113
//// finished experiment
// 127.0.0.1:xxxx> exit