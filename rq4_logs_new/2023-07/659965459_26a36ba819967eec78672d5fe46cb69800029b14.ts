import { Response } from 'express'
import { StatusCodes } from 'http-status-codes'
import Request from '../types/Request'
import { Tenant } from '../models/Tenant'
import TenantSchema from '../schemas/Tenant'

export const getTenantById = async (req: Request, res: Response): Promise<void> => {
  const { id } = req.params

  try {
    const tenant = await Tenant.findById(id)

    res.status(StatusCodes.OK).json(tenant)
  } catch (err) {
    res.status(StatusCodes.NOT_FOUND).json(err)
  }
}

export const updateTenant = async (req: Request, res: Response): Promise<void> => {
  const { error, value } = TenantSchema.validate(req.body)

  if (error) {
    res.status(StatusCodes.BAD_REQUEST).json({ msg: error.message })
    return
  }

  const { name, email } = value
  console.log(TenantSchema.validate(req.body), 'this is value')

  const tenant = await Tenant.findOne({ _id: req.tenantId })
  console.log(tenant, 'this is tenant')
  tenant.name = name
  tenant.email = email

  await tenant.save()

  const token = tenant.createJwt()
  res.status(StatusCodes.OK).json({
    tenant: {
      tenantId: tenant._id,
      name: tenant.name,
      email: tenant.email,
    },
    token,
  })
}