export interface Reward {
  id: string
  name: string
  description: string | null
  pointsCost: number
  stock: number
  monetaryValue: number | null
  isActive: boolean
  expiryDate: string | null
  imageUrl: string | null
  createdAt: string
  updatedAt: string
  _count?: {
    rewardClaims: number
  }
}

export interface RewardDetail extends Reward {
  rewardClaims?: RewardClaim[]
}

export interface RewardClaim {
  id: string
  memberId: string
  rewardId: string
  claimDate: string
  status: string
  member: {
    id: string
    name: string
  }
}

export interface RewardFilterParams {
  query?: string
  page: number
  pageSize: number
  sortBy: string
  sortDirection: 'asc' | 'desc'
  includeInactive?: boolean
}

export interface RewardFilterUIState {
  search: string
  status: string[]
}

export type RewardDialogType = 'view' | 'create' | 'edit'