import ProfileRepository from '../infra/typeorm/repositories/ProfileRepository';
import IProfileRepository from "@modules/Profile/repositories/IProfileRepository";

export default class HasProfileByEmailService {

	private profileRepository : IProfileRepository;
	private email : string;

	constructor(email : string) {
		this.profileRepository = new ProfileRepository();
		this.email = email;
	}

	async run(): Promise<boolean> {

		const profileEmail = await this.profileRepository.findProfileByEmail(this.email);
    console.log(profileEmail);
		if(!profileEmail) {
			return false;
		}

		return true;
	}
}