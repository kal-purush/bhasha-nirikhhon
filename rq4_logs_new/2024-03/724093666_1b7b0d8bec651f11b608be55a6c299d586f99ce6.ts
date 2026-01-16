class TimeRange {
  from: number; //timestamp
  to: number; //timestamp
}
export enum SortType {
  DATE_ASC = 'DATE_ASC',
  DATE_DESC = 'DATE_DESC',
  TOTAL_ASC = 'TOTAL_ASC',
  TOTAL_DESC = 'TOTAL_DESC',
}
enum HistoryOrderStatus {
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  CANCELLED = 'CANCELLED',
}
export class GetOrderHistoryByFoodRequest {
  customer_id: number;
  search_keyword: string;
  sort_type: SortType;
  filtered_order_status: HistoryOrderStatus[]; // OPTIONAL - COMPLETED | FAILED | CANCELLED
  time_range: TimeRange; // OPTIONAL
  offset: number; //REQUIRED
  page_size: number; //REQUIRED
}