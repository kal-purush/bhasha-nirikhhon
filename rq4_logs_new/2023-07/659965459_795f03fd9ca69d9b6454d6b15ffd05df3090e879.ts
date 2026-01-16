import { IReport, Report } from '../models/Report'

export const createReport = async (reportData: Partial<IReport>): Promise<IReport> => {
  const report = await Report.create(reportData)
  return report
}

export const getReportById = async (reportId: string): Promise<IReport | null> => {
  const report = await Report.findById(reportId)
  return report
}

export const updateReport = async (reportId: string, updatedData: Partial<IReport>): Promise<IReport | null> => {
  const report = await Report.findByIdAndUpdate(reportId, updatedData, { new: true })
  return report
}

export const deleteReport = async (reportId: string): Promise<IReport | null> => {
  const report = await Report.findByIdAndDelete(reportId)
  return report
}