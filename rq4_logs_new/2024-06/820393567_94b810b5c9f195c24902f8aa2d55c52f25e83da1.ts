interface TreeResponse {
	child: TreeResponse[]
	equipmentCosts: number
	estimatedProfit: number
	id: number
	machineOperatorSalary: number
	mainCosts: number
	materials: number
	mimExploitation: number
	overheads: number
	rowName: string
	salary: number
	supportCosts: number
	total: number
	parentId?: number
}

interface OutlayRowRequest {
	equipmentCosts: number
	estimatedProfit: number
	machineOperatorSalary: number
	mainCosts: number
	materials: number
	mimExploitation: number
	overheads: number
	parentId: number | null
	rowName: string
	salary: number
	supportCosts: number
}

interface OutlayRowUpdateRequest {
	equipmentCosts: number
	estimatedProfit: number
	machineOperatorSalary: number
	mainCosts: number
	materials: number
	mimExploitation: number
	overheads: number
	rowName: string
	salary: number
	supportCosts: number
}