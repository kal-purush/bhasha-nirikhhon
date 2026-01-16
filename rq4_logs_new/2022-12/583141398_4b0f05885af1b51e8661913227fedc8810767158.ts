import { hashSync } from "bcrypt";
import { instanceToPlain } from "class-transformer";
import { AppDataSource } from "../../data-source";
import { Recruiter } from "../../entities/recruiter";
import { AppError } from "../../errors/appError";
import { IRecruiterUpdate } from "../../interfaces/recruiter";


export const updateRecruiterService = async (data: IRecruiterUpdate, recruiterId: string) => {
    let checkKeys = Object.keys(data).map(
        item =>
          item.includes("name") ||
          item.includes("email") ||
          item.includes("password") ||
          item.includes("social") ||
          item.includes("avatarUrl") ||
          item.includes("company")
      );
    
    if (checkKeys.includes(false)) {
        throw new AppError("Invalid key", 401);
    }

    const recruiterRepository = AppDataSource.getRepository(Recruiter)

    const recruiter = await recruiterRepository.findOneBy({id: recruiterId})
    
    if(recruiter){
        await recruiterRepository.update(
            recruiterId,
            {
                name: data.name ? data.name : recruiter.name,
                email: data.email ? data.email : recruiter.email,
                password: data.password ? hashSync(data.password, 10) : recruiter.password,
                social: data.social ? data.social : recruiter.social,
                avatarUrl: data.avatarUrl ? data.avatarUrl : recruiter.avatarUrl,
                company: data.company ? data.company : recruiter.company,
            }
        )
    }

    const updatedRecruiter = await recruiterRepository.findOneBy({id: recruiterId})

    return instanceToPlain(updatedRecruiter)

}