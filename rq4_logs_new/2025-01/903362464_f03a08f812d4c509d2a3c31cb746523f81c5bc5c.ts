export const STATUS_STYLE = {
  pending: { msg: '예약 신청', color: '#2EB4FF', cancelAvailable: true, reviewAvailable: false },
  confirm: { msg: '예약 승인', color: '#FF7C1D', cancelAvailable: false, reviewAvailable: false },
  declined: { msg: '예약 거절', color: '#ff472e', cancelAvailable: false, reviewAvailable: false },
  canceled: { msg: '예약 취소', color: '#79747E', cancelAvailable: false, reviewAvailable: false },
  completed: { msg: '체험 완료', color: '#79747E', cancelAvailable: false, reviewAvailable: true },
};

export const STATUS_TRANSLATE = {
  '예약 신청': 'pending',
  '예약 승인': 'confirm',
  '예약 거절': 'declined',
  '예약 취소': 'canceled',
  '체험 완료': 'complete',
};