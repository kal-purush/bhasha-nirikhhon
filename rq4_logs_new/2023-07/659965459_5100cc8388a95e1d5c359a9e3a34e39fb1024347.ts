import mongoose, { Document, Model, Schema } from 'mongoose'

export interface IReport extends Document {
  title: string
}

export interface IReportModel extends Model<IReport> {
  createReport(reportData: Partial<IReport>): Promise<IReport>
  getReportById(reportId: string): Promise<IReport | null>
  updateReport(reportId: string, updatedData: Partial<IReport>): Promise<IReport | null>
  deleteReport(reportId: string): Promise<IReport | null>
}

const ReportSchema = new Schema<IReport, IReportModel>({
  title: {
    type: String,
    required: [true, 'please enter your report title.'],
  },
})

ReportSchema.statics.createReport = async function (reportData: Partial<IReport>): Promise<IReport> {
  return this.create(reportData)
}

ReportSchema.statics.getReportById = async function (reportId: string): Promise<IReport | null> {
  return this.findById(reportId).lean()
}

ReportSchema.statics.updateReport = async function (
  reportId: string,
  updatedData: Partial<IReport>
): Promise<IReport | null> {
  return this.findByIdAndUpdate(reportId, updatedData, { new: true }).lean()
}

ReportSchema.statics.deleteReport = async function (reportId: string): Promise<IReport | null> {
  return this.findByIdAndDelete(reportId).lean()
}

export const Report: IReportModel = mongoose.model<IReport, IReportModel>('Report', ReportSchema)