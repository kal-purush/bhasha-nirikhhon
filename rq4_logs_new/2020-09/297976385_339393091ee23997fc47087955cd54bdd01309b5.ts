import { Tracking } from '@src/models/tracking';
import { InternalError } from '@src/util/errors/internal-error';

export class TrackingService {
   
    public async processTracking(
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      orderBy: 'asc' | 'desc' = 'desc'
    ): Promise<Tracking[]> {
      try {
        return [];
      } catch (error) {
        throw new TrackingProcessingInternalError(error.message);
      }
    }    
  }

  export class TrackingProcessingInternalError extends InternalError {
    constructor(message: string) {
      super(`Unexpected error during the tracking processing: ${message}`);
    }
  }