import { Response, NextFunction } from 'express';
import { Request as JWTRequest } from 'express-jwt'
import { dataSource } from '../db/data-source'
import { isNotValidInteger } from '../utils/validation';
import getLogger from '../utils/logger'
import responseSend, { initResponseData } from '../utils/serverResponse'
import { getAuthUser } from '../middlewares/auth'
import { ActivityEntity } from '../entities/Activity';
import { ShowtimesEntity } from '../entities/Showtimes';
import { OrderStatus, PaymentMethod, PaymentStatus,PickupStatus, OrderEntity } from '../entities/Order';
import { OrderTicketEntity } from '../entities/OrderTicket';

import { seatInventoryService } from '../utils/seatInventory';

const logger = getLogger('User')

// 票券介面
interface TicketInput {
    zone: string;
    price: number;
    quantity: number;
}

// 訂單請求 Body 介面
interface OrderRequestBody {
    activity_id: string;
    showtime_id: string; // 場次 ID (UUID)
    tickets: TicketInput[];
}

// 基於時間產生的訂單編號
function generateOrderNumber(): string {
    return `ORD-${Date.now()}-${Math.random().toString(36).substring(2, 8).toUpperCase()}`;
}

export async function postCreateOrder(req: JWTRequest, res: Response, next: NextFunction) {
    if (!seatInventoryService) {
        logger.error('SeatInventoryService 未初始化！');
        responseSend(initResponseData(res, 5601), logger);
        return;
    }

    let queryRunner;
    const successfullyDeductedTickets: { zone: string; quantity: number }[] = [];
    let parsedActivityId: number | undefined;
    let showtimeId: string | undefined;

    try {
        queryRunner = dataSource.getRepository(ActivityEntity).manager.connection.createQueryRunner();
        await queryRunner.connect();
        await queryRunner.startTransaction(); // **開始事務**：確保訂單和訂單票券的資料庫操作原子性

        const { id } = getAuthUser(req)

        const { activity_id, showtime_id, tickets } = req.body as OrderRequestBody;
        showtimeId = showtime_id;

        // 2. Body 驗證
        if (isNotValidInteger(parseInt(activity_id, 10)) || !showtimeId || !tickets || !Array.isArray(tickets) || tickets.length === 0) {
            responseSend(initResponseData(res, 1000), logger);
            return;
        }
        parsedActivityId = parseInt(activity_id, 10);

        if (tickets.length > 4) {
            responseSend(initResponseData(res, 1601), logger);
            return;
        }

        // 檢查每筆票券的格式
        for (const ticket of tickets) {
            if (!ticket.zone || isNotValidInteger(ticket.price) || isNotValidInteger(ticket.quantity)) {
                responseSend(initResponseData(res, 1602), logger);
                return;
            }
        }

        // 3. 活動 ID 有效性檢查與狀態判斷
        const activityRepository = queryRunner.manager.getRepository(ActivityEntity);
        const showtimeRepository = queryRunner.manager.getRepository(ShowtimesEntity);

        const activity = await activityRepository.findOne({
            where: { id: parsedActivityId, is_deleted: false },
        });

        if (!activity) {
            responseSend(initResponseData(res, 1603), logger);
            return;
        }
    
        //活動場次有效性
        const showtime = await showtimeRepository.findOne({
            where: { id: showtimeId, activity_id: parsedActivityId },
            relations: ['showtimeSections']
        });

        if (!showtime) {
            responseSend(initResponseData(res, 1604), logger);
            return;
        }
        if (!showtime.showtimeSections || showtime.showtimeSections.length === 0) {
             responseSend(initResponseData(res, 1605), logger);
             return;
        }

        // 檢查活動是否在開賣中
        const now = new Date();
        if (now < activity.sales_start_time || now > activity.sales_end_time) {
            responseSend(initResponseData(res, 1606), logger);
            return;
        }

        // 4. 座位庫存判斷
        const showtimeSections = showtime.showtimeSections;
        let totalTicketsQuantity = 0; // 用於計算 total_count
        let totalPriceAmount = 0; // 用於計算 total_price

        for (const requestedTicket of tickets) {
            const sectionDetail = showtimeSections.find(section => section.section === requestedTicket.zone);

            if (!sectionDetail) {
                responseSend(initResponseData(res, 1607), logger);
                return;
            }
            // 檢查 price 是否存在且匹配
            if (sectionDetail.price === null || sectionDetail.price === undefined || sectionDetail.price !== requestedTicket.price) {
                responseSend(initResponseData(res, 1608), logger);
                return;
            }

            // 實際的座位庫存檢查和鎖定。
            const deductedSuccessfully = await seatInventoryService.deductSeats(
                showtimeId,
                requestedTicket.zone,
                requestedTicket.quantity
            );

            if (!deductedSuccessfully) {
                responseSend(initResponseData(res, 1609), logger);
                return;
            }
            // 記錄成功預扣的票券
            successfullyDeductedTickets.push({ zone: requestedTicket.zone, quantity: requestedTicket.quantity });

            totalTicketsQuantity += requestedTicket.quantity;
            totalPriceAmount += requestedTicket.price * requestedTicket.quantity;
        }

        // 5. 建立訂單
        const orderRepository = queryRunner.manager.getRepository(OrderEntity);
        const orderTicketRepository = queryRunner.manager.getRepository(OrderTicketEntity);

        const newOrder = new OrderEntity();
        newOrder.user_id = id;
        newOrder.showtime_id = showtimeId;
        newOrder.order_number = generateOrderNumber();
        newOrder.status = OrderStatus.PROCESSING;
        newOrder.total_count = totalTicketsQuantity;
        newOrder.total_price = totalPriceAmount;
        newOrder.payment_method = PaymentMethod.CREDIT_CARD;
        newOrder.payment_status = PaymentStatus.PENDING;
        newOrder.pickup_status = PickupStatus.NOT_PICKED_UP;

        const savedOrder = await orderRepository.save(newOrder);

        const orderTicketsToSave: OrderTicketEntity[] = tickets.map(ticket => {
            const orderTicket = new OrderTicketEntity();
            orderTicket.order_id = savedOrder.id;
            orderTicket.section_id = ticket.zone;
            orderTicket.price = ticket.price;
            orderTicket.quantity = ticket.quantity;
            orderTicket.ticket_type = 1;
            return orderTicket;
        });

        await orderTicketRepository.save(orderTicketsToSave);

        // 如果所有資料庫操作都成功，提交事務(原子操作)
        await queryRunner.commitTransaction();

        // 訂單建立成功
        responseSend(initResponseData(res, 200, {
            message: '訂單成立',
            status: true,
            data: {
                order_id: savedOrder.order_number
            }
        }), logger);

    } catch (error) {
        logger.error('postCreateOrder 錯誤:', error);
        // 如果成功地啟動了資料庫事務，並且這個事務目前還沒被成功提交或回滾，
        // 那麼我就嘗試回滾它，以確保資料庫的數據回到 "錯誤發生前" 的狀態。
        if (queryRunner && queryRunner.isTransactionActive) {
            try {
                await queryRunner.rollbackTransaction();
                logger.info('資料庫事務已回滾。');
            } catch (rollbackError) {
                logger.error(`資料庫事務回滾失敗: ${rollbackError instanceof Error ? rollbackError.message : String(rollbackError)}`);
            }
        }

        if (showtimeId && !isNotValidInteger(parsedActivityId) ) {
            // 將 Redis 中預扣的座位歸還
            for (const deducted of successfullyDeductedTickets) {
                try {
                    await seatInventoryService.returnSeats(showtimeId, deducted.zone, deducted.quantity);
                    logger.info(`錯誤時歸還 Redis 座位：場次 ${showtimeId} 區域 ${deducted.zone} 數量 ${deducted.quantity}`);
                } catch (returnError) {
                    logger.error(`歸還 Redis 座位失敗：${returnError instanceof Error ? returnError.message : String(returnError)}`);
                }
            }
        }

        responseSend(initResponseData(res, 400, {
            message: '訂單失敗',
            status: false,
            data: {}
        }), logger);

        next(error);
    } finally {
        if (queryRunner) {
            try {
                await queryRunner.release();
                logger.info('QueryRunner 已釋放。');
            } catch (releaseError) {
                logger.error(`釋放 QueryRunner 失敗: ${releaseError instanceof Error ? releaseError.message : String(releaseError)}`);
            }
        }
    }
}