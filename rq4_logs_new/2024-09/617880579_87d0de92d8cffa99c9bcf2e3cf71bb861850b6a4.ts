import { NextFunction, Request, Response } from 'express';
import onlyofficeService from '@/services/onlyoffice.service';
import apiService from '@/services/api.service';

/**
 * Health response for devops operational purposes. Should not be exposed.
 */
export const HealthStatusController = {
  async get(req: Request<{}, {}, {}, {}>, res: Response, next: NextFunction): Promise<void> {
    Promise.all([onlyofficeService.getLatestLicence(), apiService.hasToken(), onlyofficeService.getForgottenList()]).then(
      ([onlyOfficeLicense, twakeDriveToken, forgottenKeys]) =>
        res.status(onlyOfficeLicense && twakeDriveToken ? 200 : 500).send({
          uptimeS: Math.floor(process.uptime()),
          onlyOfficeLicense,
          twakeDriveToken,
          forgottenCount: forgottenKeys?.length ?? 0,
        }),
      err => res.status(500).send(err),
    );
  },
};