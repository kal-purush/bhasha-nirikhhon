import { Donations } from "../models/donations";
import { DonationsRepository } from "../repositories/donations.repository";
export declare class DonationsController {
    private donationsRepo;
    constructor(donationsRepo: DonationsRepository);
    newDonation(donation: Donations): Promise<Donations>;
}