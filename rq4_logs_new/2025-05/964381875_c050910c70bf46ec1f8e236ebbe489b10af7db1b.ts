import Redis from 'ioredis';
import getLogger from './logger'; // 假設您有 logger

const logger = getLogger('SeatInventoryService');

export class SeatInventoryService {
    private readonly redis: Redis;
    // 使用 hash 儲存場次區域的庫存
    // Key 格式: `showtime_seats:{showtime_id}`
    // Field 格式: `zone_name` (e.g., "VIP", "B")
    // Value: 可用座位數
    private readonly REDIS_SHOWTIME_SEATS_PREFIX = 'showtime_seats';

    constructor(redis: Redis) {
        this.redis = redis;
    }

    /**
     * 活動場次創建或更新。
     * @param showtimeId 場次 ID
     * @param zones 包含區域名稱和總容量的陣列
     * [{ section: "VIP", capacity: 100 }, { section: "B", capacity: 200 }]
     */
    async initializeActivitySeats(showtimeId: string, zones: { section: string; capacity: number }[]): Promise<void> {
        try {
            const redisKey = `${this.REDIS_SHOWTIME_SEATS_PREFIX}:${showtimeId}`;
            const multi = this.redis.multi();

            // 清空舊的庫存 (如果存在)
            multi.del(redisKey);

            // 設置新的庫存
            zones.forEach(zone => {
                multi.hset(redisKey, zone.section, zone.capacity);
            });

            await multi.exec();
            logger.info(`成功初始化場次 ${showtimeId} 的座位庫存到 Redis.`);
        } catch (error) {
            logger.error(`初始化場次 ${showtimeId} 座位庫存失敗: ${error instanceof Error ? error.message : String(error)}`);
            throw error;
        }
    }

    /**
     * 嘗試預扣指定場次區域的座位。
     * @param showtimeId 場次 ID (UUID)
     * @param zone 區域名稱
     * @param quantity 欲預扣的數量
     * @returns 預扣成功返回 true，否則返回 false (座位不足)
     */
    async deductSeats(showtimeId: string, zone: string, quantity: number): Promise<boolean> {
        try {
            const redisKey = `${this.REDIS_SHOWTIME_SEATS_PREFIX}:${showtimeId}`;

            const remainingSeats = await this.redis.hincrby(redisKey, zone, -quantity);

            if (remainingSeats < 0) {
                // 如果減少後變為負數，表示座位不足，需要將其加回並返回失敗
                await this.redis.hincrby(redisKey, zone, quantity); // 加回去
                logger.warn(`場次 ${showtimeId} 區域 ${zone} 座位不足。`);
                return false;
            }

            logger.info(`成功預扣場次 ${showtimeId} 區域 ${zone} 的 ${quantity} 個座位。剩餘：${remainingSeats}`);
            return true;

        } catch (error) {
            logger.error(`預扣場次 ${showtimeId} 區域 ${zone} 的 ${quantity} 個座位失敗: ${error instanceof Error ? error.message : String(error)}`);
            throw error;
        }
    }

    /**
     * 歸還指定場次區域的座位。
     * @param showtimeId 場次 ID (UUID)
     * @param zone 區域名稱
     * @param quantity 欲歸還的數量
     */
    async returnSeats(showtimeId: string, zone: string, quantity: number): Promise<void> {
        try {
            const redisKey = `${this.REDIS_SHOWTIME_SEATS_PREFIX}:${showtimeId}`;
            await this.redis.hincrby(redisKey, zone, quantity);
            logger.info(`成功歸還場次 ${showtimeId} 區域 ${zone} 的 ${quantity} 個座位。`);
        } catch (error) {
            logger.error(`歸還場次 ${showtimeId} 區域 ${zone} 的 ${quantity} 個座位失敗: ${error instanceof Error ? error.message : String(error)}`);
            throw error;
        }
    }

    /**
     * 獲取指定場次區域的當前可用座位數。
     * @param showtimeId 場次 ID (UUID)
     * @param zone 區域名稱
     * @returns 可用座位數，如果區域不存在則返回 0
     */
    async getAvailableSeats(showtimeId: string, zone: string): Promise<number> {
        try {
            const redisKey = `${this.REDIS_SHOWTIME_SEATS_PREFIX}:${showtimeId}`;
            const seats = await this.redis.hget(redisKey, zone);
            return seats ? parseInt(seats, 10) : 0;
        } catch (error) {
            logger.error(`獲取場次 ${showtimeId} 區域 ${zone} 可用座位失敗: ${error instanceof Error ? error.message : String(error)}`);
            throw error;
        }
    }

    /**
     * 基於 Redlock 的單一資源鎖定，用於更複雜的資源管理。
     * 庫存計數，通常直接使用原子操作 (HINCRBY)。
     * 這個方法可用於確保只有一個執行緒能修改某個特定區域的座位狀態（例如，在複雜的選座邏輯中）。
     * @param resourceName 資源名稱 (例如: `lock:activity:{id}:zone:{name}`)
     * @param ttl 鎖的過期時間 (毫秒)
     * @returns 成功獲取鎖返回 true，否則返回 false
     */
    async acquireLock(resourceName: string, ttl: number): Promise<boolean> {
        try {
            // 使用 SETNX (SET if Not eXists) 和 EX (Expire) 實現鎖定
            // `NX`: Only set the key if it does not already exist.
            // `PX`: Set the specified expire time, in milliseconds.
            const result = await this.redis.set(resourceName, 'locked', 'PX', ttl, 'NX');
            return result === 'OK';
        } catch (error) {
            logger.error(`獲取鎖定 ${resourceName} 失敗: ${error instanceof Error ? error.message : String(error)}`);
            return false;
        }
    }

    /**
     * 釋放鎖。
     * @param resourceName 資源名稱
     * @returns 成功釋放鎖返回 true，否則返回 false
     */
    async releaseLock(resourceName: string): Promise<void> {
        try {
            await this.redis.del(resourceName);
            logger.info(`成功釋放鎖定 ${resourceName}`);
        } catch (error) {
            logger.error(`釋放鎖定 ${resourceName} 失敗: ${error instanceof Error ? error.message : String(error)}`);
            throw error;
        }
    }
}

export let seatInventoryService: SeatInventoryService;
export function initSeatInventoryService(redis: Redis) {
    seatInventoryService = new SeatInventoryService(redis);
}