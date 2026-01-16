export interface INewAppointment {
  title: string,
  description: string,
  from: Date,
  to: Date,
  participants: ReadonlyArray<string>
}
export interface IAppointment extends INewAppointment {
  id: string
}