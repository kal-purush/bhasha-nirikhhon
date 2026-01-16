import { RECURRING_INVOICES } from './__generated__/RECURRING_INVOICES';
import { FailedInvoices } from './RecurringInvoices';

export const getFailedInvoicesData = (data: RECURRING_INVOICES | undefined) => {
  if (!data?.berthLeases) return [];
  return data.berthLeases.edges.reduce<FailedInvoices[]>((acc, edge) => {
    if (!edge?.node) return acc;

    const failedInvoice = {
      id: edge.node.id,
      customerId: edge.node.customer.id,
      customerName: `${edge.node.customer.firstName} ${edge.node.customer.lastName}`,
      harbor: edge.node.berth.pier.properties?.harbor.properties?.name ?? '',
      failureReason: edge.node.comment,
    };

    return [...acc, failedInvoice];
  }, []);
};