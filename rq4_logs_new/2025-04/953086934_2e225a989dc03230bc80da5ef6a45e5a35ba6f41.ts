import jsPDF from "jspdf";
import autoTable from "jspdf-autotable";
import { theme } from "@/config/theme";
import type { EstimateCalculatorValues } from "../types";

/**
 * Generate a PDF document for the estimate calculator.
 * @param data - The data to be included in the PDF.
 * @returns The generated PDF document.
 */
export const createEstimateCalculatorDoc = (data: EstimateCalculatorValues) => {
  const materialTotal = data.materials.reduce(
    (acc, { value, count }) => acc + value * (count ?? 0),
    0
  );
  const subtotal = materialTotal + (data.additional ?? 0);
  const total = subtotal + (subtotal * data.tax) / 100;

  const doc = new jsPDF();

  // Meta
  autoTable(doc, {
    headStyles: { fillColor: theme.palette.primary.main },
    head: [["Customer Information"]],
    body: [[data.name], [data.address]],
  });

  // Materials
  autoTable(doc, {
    headStyles: { fillColor: theme.palette.primary.main },
    head: [["Material", "Cost", "Quantity"]],
    body: data.materials
      .filter(({ count }) => !!count)
      .map((material) => [
        material.label,
        material.value.toUSD(),
        (material.count ?? 0).toString(),
      ]),
  });

  // Additional & Tax
  autoTable(doc, {
    columnStyles: {
      0: { cellWidth: "auto", halign: "right" },
      1: { cellWidth: 30, halign: "right" },
    },
    body: [
      ...(data.additional
        ? [["Additional Costs", data.additional.toUSD()]]
        : []),
      ["Tax", `${data.tax.toString()}%`],
    ],
  });

  // Subtotal & Total
  autoTable(doc, {
    columnStyles: {
      0: { cellWidth: "auto", halign: "right" },
      1: { cellWidth: 50, halign: "right" },
    },
    styles: { fontSize: 12, fontStyle: "bold" },
    body: [[`Subtotal:   ${subtotal.toUSD()}`], [`Total:   ${total.toUSD()}`]],
  });

  return doc;
};