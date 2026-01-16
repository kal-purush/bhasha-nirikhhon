import { TData } from 'types/DataTypes'

export class Project {
    id?: number
    name: string
    description: string
    imageUrl?: string
    contractTypeId?: number
    contractSignedOn?: string
    budget: number
    isActive?: boolean

    constructor(initializer?: TData) {
        this.id = initializer?.id
        this.name = initializer?.name || ''
        this.description = initializer?.description || ''
        this.imageUrl = initializer?.imageUrl || undefined
        this.contractTypeId = initializer?.contractTypeId
        this.contractSignedOn = initializer?.contractSignedOn
            ? new Date(initializer.contractSignedOn).toISOString()
            : new Date().toISOString()
        this.budget = initializer?.budget || 0
        this.isActive = initializer?.isActive || false
    }
}