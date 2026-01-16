import { JobPositionDetail } from '@/models';
import { AxiosResponse } from 'axios';

type ServiceFunction<T> = (payload: T) => Promise<AxiosResponse<any, any>>;

export interface UserLoginPayload {
  username: string;
  password: string;
}

export interface UserRegisterPayload {
  username: string;
  password: string;
  name: string;
  phoneNumber: string;
  email: string;
  role: string;
}

export interface UserChangePasswordPayload {
  username: string;
  prevPassword: string;
  newPassword: string;
}

interface UserUpdateInfoPayload {
  username: string;
  name: string;
  phoneNumber: string;
  role: string;
  email: string;
}

export interface JobHunterUpdateInfoPayload extends UserUpdateInfoPayload {
  jobType: string;
  jobTag: string[];
  university: string;
  education: string;
  salaryRange: number[];
  userType: string;
}

export interface RecruiterUpdateInfoPayload extends UserUpdateInfoPayload {
  company: string;
  department: string;
}

export interface UserUploadAvatarPayload {
  username: string;
  avatar: File;
}

export interface UserUploadResumePayload {
  username: string;
  resume: File;
}

/** 只需要用户名的公用 PayLoad 类型 */
export interface UsernamePayload {
  username: string;
}

export interface PostJobPayload {
  username: string;
  jobPositionDetail: JobPositionDetail;
}

export interface ChangeJobPayload {
  jobPositionDetail: JobPositionDetail;
}

export interface DeleteJobPayload {
  jobId: string;
}

/** 需要用户名分页信息的 Payload 类型 */
export interface UsernameWithPagePayload {
  username: string;
  pageSize: number;
  pageNumber: number;
}

export interface SearchResumeReceivePayload {
  username: string;
  jobId: string;
  jobTitle: string;
  candidateName: string;
  pageSize: number;
  pageNumber: number;
}

export interface ChangeResumeStatusPayload {
  recordId: string;
}

export interface Service {
  userLogin: ServiceFunction<UserLoginPayload>;
  userRegister: ServiceFunction<UserRegisterPayload>;
  userChangePassword: ServiceFunction<UserChangePasswordPayload>;
  userUpdateInfo: ServiceFunction<JobHunterUpdateInfoPayload | RecruiterUpdateInfoPayload>;
  userUploadAvatar: ServiceFunction<UserUploadAvatarPayload>;
  userUploadResume: ServiceFunction<UserUploadResumePayload>;
  userRecommendJobs: ServiceFunction<UsernamePayload>;
  userJobStars: ServiceFunction<UsernamePayload>;
  userJobRecords: ServiceFunction<UsernamePayload>;
  getAllPostJobs: ServiceFunction<UsernamePayload>;
  postJob: ServiceFunction<PostJobPayload>;
  changeJob: ServiceFunction<ChangeJobPayload>;
  deleteJob: ServiceFunction<DeleteJobPayload>;
  getAllResumeReceive: ServiceFunction<UsernameWithPagePayload>;
  searchResumeReceive: ServiceFunction<SearchResumeReceivePayload>;
  changeResumeStatus: ServiceFunction<ChangeResumeStatusPayload>;
}