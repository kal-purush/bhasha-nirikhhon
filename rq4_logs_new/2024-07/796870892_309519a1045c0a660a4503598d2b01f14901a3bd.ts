import { RoutesProps } from "@/interfaces/route";
import InvoiceListScreen from "@/pages/invoices/InvoiceListScreen";
export const INVOICES_ROUTES: RoutesProps[] = [
  {
    component: InvoiceListScreen,
    url: "/invoices",
    requireAuth: true
  }
];