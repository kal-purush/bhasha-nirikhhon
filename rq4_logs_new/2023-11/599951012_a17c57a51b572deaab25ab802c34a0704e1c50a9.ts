/**
 * 部门数据结构
 */
interface Department {
  /** 部门 ID */
  id: number
  /** 部门标题 */
  title: string
  /** 部门描述 */
  description: string
  /** 数据创建时间 */
  createdAt: string
  /** 数据更新时间 */
  updatedAt: string
}

export default Department