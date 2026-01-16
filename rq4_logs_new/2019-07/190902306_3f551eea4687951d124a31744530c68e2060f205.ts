import { HttpException, HttpStatus } from '@nestjs/common'

import { CompanyDto } from '../dto/company.dto'
import { Company } from '../company.entity'

export const mockCompanyDto: CompanyDto = {
    name: 'name',
    logo: 'logo',
    description: 'desc'
}

export const mockUpdateCompanyDto: CompanyDto = {
    name: 'name2',
    logo: 'logo2',
    description: 'desc2'
}

export const mockCompany: Company = {
    id: 1,
    name: 'name',
    logo: 'logo',
    description: 'desc',
    userId: 1,
    castings: []
}

export const mockCompanies = [mockCompany, mockCompany]

export const notValidUserException = new HttpException(
    'Not valid user',
    HttpStatus.NOT_FOUND
)

export const deleteCompanyFailedException = new HttpException(
    'Delete company failed',
    HttpStatus.CONFLICT
)

export const userId = 1
export const otherUserId = 2