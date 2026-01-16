import {describe, expect, test} from "@jest/globals";
import {prismaMock} from "./singleton_prisma";
import {simulateBillPaymentAction} from "@/app/(internal)/payments/bill-action";
import {Bill, Prisma} from "@prisma/client";

describe('BillAction', () => {
  describe('test simulateBillPaymentAction', () => {
    test('balance is greater than outstanding', async () => {
      const today = new Date();
      const bills: Partial<Bill>[] = [
        {
          amount: new Prisma.Decimal(500000),
          due_date: new Date(today.getFullYear(), today.getMonth() - 2, today.getDate()),
          paid_amount: null,
        },
        {
          amount: new Prisma.Decimal(500000),
          due_date: new Date(today.getFullYear(), today.getMonth() - 1, today.getDate()),
          paid_amount: null,
        },
        {
          amount: new Prisma.Decimal(500000),
          due_date: today,
          paid_amount: null,
        }
      ];

      prismaMock.bill.findMany.mockResolvedValue(bills);

      let resp = await simulateBillPaymentAction(2000000, 1);

      expect(resp.balance)
        .toBe(500000);

      resp.bookings.forEach((booking) => {
        expect(booking.paid_amount?.toNumber())
          .toEqual(booking.amount.toNumber());
      });
    });

    test('balance is less than outstanding', async () => {
      const today = new Date();
      const bills: Partial<Bill>[] = [
        {
          amount: new Prisma.Decimal(500000),
          due_date: new Date(today.getFullYear(), today.getMonth() - 2, today.getDate()),
          paid_amount: null,
        },
        {
          amount: new Prisma.Decimal(500000),
          due_date: new Date(today.getFullYear(), today.getMonth() - 1, today.getDate()),
          paid_amount: null,
        },
        {
          amount: new Prisma.Decimal(500000),
          due_date: today,
          paid_amount: null,
        }
      ];

      prismaMock.bill.findMany.mockResolvedValue(bills);

      let resp = await simulateBillPaymentAction(1250000, 1);

      expect(resp.balance)
        .toBe(0);

      expect(resp.bookings[0].paid_amount?.toNumber())
        .toBe(resp.bookings[0].amount.toNumber());

      expect(resp.bookings[1].paid_amount?.toNumber())
        .toBe(resp.bookings[1].amount.toNumber());

      expect(resp.bookings[2].paid_amount?.toNumber())
        .toBe(250000);
    });

    test('partially paid bill', async () => {
      const today = new Date();
      const bills: Partial<Bill>[] = [
        {
          amount: new Prisma.Decimal(500000),
          due_date: new Date(today.getFullYear(), today.getMonth() - 2, today.getDate()),
          paid_amount: new Prisma.Decimal(250000),
        },
        {
          amount: new Prisma.Decimal(500000),
          due_date: new Date(today.getFullYear(), today.getMonth() - 1, today.getDate()),
          paid_amount: new Prisma.Decimal(300000),
        },
        {
          amount: new Prisma.Decimal(500000),
          due_date: today,
          paid_amount: new Prisma.Decimal(350000),
        }
      ];

      prismaMock.bill.findMany.mockResolvedValue(bills);

      let resp = await simulateBillPaymentAction(1500000, 1);

      expect(resp.balance)
        .toBe(900000);

      resp.bookings.forEach((booking) => {
        expect(booking.paid_amount?.toNumber())
          .toEqual(booking.amount.toNumber());
      });
    });

    test('fully paid bill', async () => {
      const today = new Date();
      const bills: Partial<Bill>[] = [
        {
          amount: new Prisma.Decimal(500000),
          due_date: new Date(today.getFullYear(), today.getMonth() - 2, today.getDate()),
          paid_amount: new Prisma.Decimal(500000),
        },
        {
          amount: new Prisma.Decimal(500000),
          due_date: new Date(today.getFullYear(), today.getMonth() - 1, today.getDate()),
          paid_amount: new Prisma.Decimal(500000),
        },
        {
          amount: new Prisma.Decimal(500000),
          due_date: today,
          paid_amount: new Prisma.Decimal(500000),
        }
      ];

      prismaMock.bill.findMany.mockResolvedValue(bills);

      let resp = await simulateBillPaymentAction(1500000, 1);

      expect(resp.balance)
        .toBe(1500000);

      resp.bookings.forEach((booking) => {
        expect(booking.paid_amount?.toNumber())
          .toEqual(booking.amount.toNumber());
      });
    });

    test('only return bills affected', async () => {
      const today = new Date();
      const bills: Partial<Bill>[] = [
        {
          amount: new Prisma.Decimal(500000),
          due_date: new Date(today.getFullYear(), today.getMonth() - 2, today.getDate()),
          paid_amount: null,
        },
        {
          amount: new Prisma.Decimal(500000),
          due_date: new Date(today.getFullYear(), today.getMonth() - 1, today.getDate()),
          paid_amount: null,
        },
        {
          amount: new Prisma.Decimal(500000),
          due_date: today,
          paid_amount: null,
        }
      ];
      prismaMock.bill.findMany.mockResolvedValue(bills);

      let resp = await simulateBillPaymentAction(250000, 1);

      expect(resp.balance)
        .toBe(0);

      expect(resp.bookings.length)
        .toBe(1);

      expect(resp.bookings[0].paid_amount!.toNumber())
        .toBe(250000);
    });
  });
});