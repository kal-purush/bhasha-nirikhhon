export const OriginData = [
  { name: "EU", available: true },
  { name: "HORS_EU", available: true },
] as const

export const TerritoryData = [
  { name: "CORSE", available: false },
  { name: "FRANCE", available: false },
  { name: "GUADELOUPE", available: false },
  { name: "GUYANE", available: false },
  { name: "MARTINIQUE", available: false },
  { name: "MAYOTTE", available: false },
  { name: "REUNION", available: true },
] as const

export const TransporterData = [
  { name: "CHRONOPOST", available: false },
  { name: "COLISSIMO", available: true },
  { name: "DHL", available: false },
  { name: "FEDEX", available: false },
  { name: "UPS", available: false },
] as const

export type Origin = (typeof OriginData)[number]["name"]
export type Territory = (typeof TerritoryData)[number]["name"]
export type Transporter = (typeof TransporterData)[number]["name"]