import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

i18n
  .use(initReactI18next)
  .init({
    debug: true,
    fallbackLng: 'en',
    interpolation: {
      escapeValue: false,
    },
    resources: {
      Zn: {
        translation: {
          POManagement: {
            title: 'PO訂單管理系統',
            ChangeLanguage:'改變語言',
            DateFilter: '日期過濾器',
            FactoryFilter: '工廠過濾器',
            POStatusFilter: '採購訂單狀態篩選器',
            OrderAgainstPO: '針對 PO 的訂單',
            RecordsIsNotAvailableForAboveFilter: '記錄不適用於上述過濾器',
            ManualPO: '手冊PO',
            ScheduledPO:'預定訂單',
            PONo:'波諾',
            Date:'日期',
            Quantity:'數量',
            RMBPrice:'人民幣價格',
            AEDPrice:'迪拉姆價格',
            POStatus:'採購訂單狀態',
            PaymentStatus:'支付狀態',
            Factory:'工廠',
            ViewItem:'查看項目',
          }
        }
      },
      En: {
        translation: {
          POManagement: {
            title: 'PO Order Management System',
            ChangeLanguage:'改變語言',
            DateFilter: 'Date Filter',
            FactoryFilter: 'Factory Filter',
            POStatusFilter: 'PO Status Filter',
            OrderAgainstPO: 'Order Agains PO',
            RecordsIsNotAvailableForAboveFilter: 'Records Is Not Available For Above Filter',
            ManualPO: 'Manual PO',
            ScheduledPO:'Scheduled PO',
            PONo:'PO No.',
            Date:'Date',
            Quantity:'Quantity',
            RMBPrice:'RMB Price',
            AEDPrice:'AED Price',
            POStatus:'PO Status',
            PaymentStatus:'Payment Status',
            Factory:'Factory',
            ViewItem:'View Item',
          }
        }
      },
    }
  });

export default i18n;