import { IReservation, ReservationStatus } from '../interface/reservation';
import mongoose, { Schema } from 'mongoose';

const ReservationSchema = new Schema<IReservation>({
	restaurant_slug: {
		type: String,
		required: true,
	},
	table_slug: {
		type: String,
		required: true,
	},
	user_id: {
		type: String,
		required: true,
	},
	reservation_time: {
		type: Date,
		required: true,
	},
	status: {
		type: String,
		enum: Object.values(ReservationStatus),
		default: ReservationStatus.PENDING,
		required: true,
	},
	date: {
		type: Date,
		default: Date.now,
	},
	guest_count: {
		type: Number,
		required: true,
	},
});

const Reservation = mongoose.model<IReservation>('Reservation', ReservationSchema);

export default Reservation;