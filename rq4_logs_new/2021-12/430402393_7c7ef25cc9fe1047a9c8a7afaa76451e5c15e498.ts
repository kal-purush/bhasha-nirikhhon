import { Role } from "@/enums";

/**
 * 用户基本信息
 */
export interface UserInfo {
  /**
   * 用户名，用户的唯一标识
   */
  username: string;

  /**
   * 用户角色, 区分为未登录 (not_logged), 求职者 (job_hunter), 招聘者 (recruiter) 三种
   */
  role: Role;

  /**
   * 真实姓名
   */
  name: string;

  /** 
   * 电话号码
   */
  phoneNumber: string;

  /**
   * 邮箱地址
   */
  email: string;
}

/**
 * 求职者
 */
export interface JobHunterInfo extends UserInfo {
  /**
   * 用户期望寻求的职位类型
   */
  jobType: string;

  /**
   * 求职标签
   */
  jobTag: string[] | undefined;

  /**
   * 毕业院校
   */
  university: string;

  /**
   * 最高学历
   */
  education: string;

  /**
   * 期望工作城市
   */
  city: string;

  /**
   * 期望薪资范围, 单位为千 (k), 例: [1, 5] 表示 1k-5k
   */
  salaryRange: number[];

  /**
   * 求职类型, 分为校招 ('campus') 和社招 ('society')
   */
  userType: string;

  /**
   * 简历链接地址 (可选)
   */
  resumeUrl?: string;
}

/**
 * 招聘者
 */
export interface RecruiterInfo extends UserInfo {
  /**
   * 所在公司名称
   */
  company: string;

  /**
   * 所在部门名称
   */
  department: string;
}

/**
 * 公司信息
 */
export interface Company {
  /**
   * 公司名称, 是公司的唯一标识
   */
  name: string;

  /**
   * 简介
   */
  description: string;

  /**
   * Logo 链接
   */
  logoUrl: string;

  /**
   * 官网地址
   */
  officialLink: string;

  /**
   * 地点
   */
  location: string | string[];

  /**
   * 在招职位数
   */
  jobNumber: number;
}

/**
 * 职位信息
 */
export interface JobPosition {
  /**
   * 职位名称
   */
  title: string;

  /**
   * 职位 ID
   */
  id: string;

  /**
   * 发布时间 (UTC+8), 约定格式为 YYYY-MM-dd HH:mm
   */
  postTime: string

  /**
   * 工作地点
   */
   location: string | string[];

  /**
   * 经验要求 (可选), 若为空则表示不限
   */
  experienceRequirement?: string;

  /**
   * 学历要求 (可选), 若为空则表示不限
   */
  educationRequirement?: string;

  /**
   * 薪资范围, 与 JobHunterInfo 中 salaryRange 格式相同
   */
  salaryRange: number[];

  /**
   * 公司名称
   */
  company: string;

  /**
   * 职位所在部门
   */
  department: string;

  /**
   * 公司 Logo 链接
   */
  logoUrl: string;
}