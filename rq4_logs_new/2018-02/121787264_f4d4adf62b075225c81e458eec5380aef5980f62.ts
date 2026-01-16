export class Classroom {
  constructor(
    public id: number,
    public className: string,
    public reservationStatus: string,
    public noOfParticipants: number,
    public capacity: number
  ) { }
}