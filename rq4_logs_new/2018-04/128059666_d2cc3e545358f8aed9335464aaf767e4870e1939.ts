export const DOCUMENT_INFORMATION_LABELS = {
  organization: {
    label: "Afdeling",
    path: ["organization", "name"],
  },
  documentType: {
    label: "Document Type",
    path: ["documentType", "name"],
  },
  referenceDate: {
    label: "Referentiedatum",
    type: "date",
  },
  uploadDate: {
    label: "Uploaddatum",
    type: "date",
  },
  fileUploadState: {
    label: "Status",
  },
}

export const DOCUMENT_META_LABELS = {
  personnelNumber: {
    label: "Personeelsnummer",
    path: ["metaData", "personnelNumber"],
  },
  brutoWage: {
    label: "Bruttoloon",
    type: "valuta",
    path: ["metaData", "wage", "bruto"],
  },
  nettoWage: {
    label: "Nettoloon",
    type: "valuta",
    path: ["metaData", "wage", "netto"],
  }
}