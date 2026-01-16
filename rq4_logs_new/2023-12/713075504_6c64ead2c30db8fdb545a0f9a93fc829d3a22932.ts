import { type Organization } from "~/server/db/schema";

export class TooManyInvoices extends Error {
  constructor() {
    super("Too many invoices");
  }
}

/**
 * @param lastNumber number of last invoice, null if no invoices exist
 * @param numbering type of numbering
 *  - `monthly` numbering the invoice number is a 10 digit number.
 * Each month has its own numbering, invoice number is a 10 digit number (2023020001, 2023020002, 2023030001)
 * - `yearly` numbering the invoice number is a 10 digit number.
 * Each year has its own numbering, invoice number is a 10 digit number (2023000001, 2023000002, 2024000001)
 * - `sequential` numbering the invoice number is a 10 digit number.
 * Just an incrementing number, invoice number is a 10 digit number (0000000001, 0000000002, 0000000003)
 */
export function getNextInvoiceNumber(
  numbering: Organization["invoiceNumbering"],
  lastNumber: string | null,
) {
  if (numbering === "monthly") {
    if (!lastNumber)
      return `${new Date().getFullYear()}${String(
        new Date().getMonth() + 1,
      ).padStart(2, "0")}0001`;

    const currentMonth = new Date().getMonth() + 1;
    const currentYear = new Date().getFullYear();

    const lastNumberMonth = Number(lastNumber.slice(4, 6));
    const lastNumberYear = Number(lastNumber.slice(0, 4));

    if (currentMonth !== lastNumberMonth || currentYear !== lastNumberYear)
      return `${currentYear}${String(currentMonth).padStart(2, "0")}0001`;

    const noOfInvoices = Number(lastNumber.slice(6));
    if (noOfInvoices >= 9999) {
      throw new TooManyInvoices();
    }

    return String(Number(lastNumber) + 1).padStart(10, "0");
  }

  if (numbering === "yearly") {
    if (!lastNumber) return `${new Date().getFullYear()}00000001`;

    const currentYear = new Date().getFullYear();
    const lastNumberYear = Number(lastNumber.slice(0, 4));

    if (currentYear !== lastNumberYear) return `${currentYear}000001`;

    const noOfInvoices = Number(lastNumber.slice(4));
    if (noOfInvoices >= 999999) {
      throw new TooManyInvoices();
    }

    return String(Number(lastNumber) + 1).padStart(8, "0");
  }

  if (!lastNumber) return "0000000001";

  const noOfInvoices = Number(lastNumber);
  if (noOfInvoices >= 9999999999) {
    throw new TooManyInvoices();
  }

  return String(Number(lastNumber) + 1).padStart(10, "0");
}