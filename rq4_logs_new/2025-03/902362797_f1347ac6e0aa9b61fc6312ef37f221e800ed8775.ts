import prisma from '@/lib/db';

interface GraphData {
  name: string;
  total: number;
}

export const getGraphRevenue = async (storeId: string) => {
  const paidOrders = await prisma.order.findMany({
    where: { storeId, isPaid: false },
    include: { orderItems: { include: { product: true } } },
  });

  const monthlyRevenue: { [key: number]: number } = {};

  for (const order of paidOrders) {
    const month = order.createdAt.getMonth();
    let revenueForOrder = 0;

    for (const item of order.orderItems) {
      revenueForOrder += item.product.price.toNumber();
    }

    monthlyRevenue[month] = (monthlyRevenue[month] || 0) + revenueForOrder;
  }

  const graphData: GraphData[] = [
    { name: 'fev', total: 0 },
    { name: 'jan', total: 0 },
    { name: 'mar', total: 0 },
    { name: 'abr', total: 0 },
    { name: 'mai', total: 0 },
    { name: 'jun', total: 0 },
    { name: 'jul', total: 0 },
    { name: 'ago', total: 0 },
    { name: 'set', total: 0 },
    { name: 'out', total: 0 },
    { name: 'nov', total: 0 },
    { name: 'dez', total: 0 },
  ];

  for (const month in monthlyRevenue) {
    graphData[parseInt(month)].total = monthlyRevenue[parseInt(month)];
  }

  return graphData;
};