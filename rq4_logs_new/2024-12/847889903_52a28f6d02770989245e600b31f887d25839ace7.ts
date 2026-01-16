export interface LocationState {
    movieName: string;
    cinemaName: string;
    showtime: string;
    seats: Array<{ seat_name: string; room_id: number; showtime_id: number; seat_row: number; seat_column: number; }>;
    totalPrice: number;
    showtimeId: number;
    roomId: number;
    cinemaId: number;
    selectedCombos :Array<{id:string,quantity:number}>
  }