import SeverityLevelService from '../../services/postgresql/SeverityLevelService';
import {Request, ResponseToolkit} from '@hapi/hapi';

export class SeverityLevelHandler {
  private severityLevelService: SeverityLevelService;
  constructor(severityLevelService: SeverityLevelService) {
    this.severityLevelService = severityLevelService;
  }

  postSeverityLevel = async (request: Request, h: ResponseToolkit) => {
    const {name, level} = request.payload as {name: string; level: number};

    const newLevel = await this.severityLevelService.add(name, level);

    return h
      .response({
        status: 'success',
        message: 'New level have been added successfully',
        data: {
          level: newLevel,
        },
      })
      .code(201);
  };

  getAllSeverityLevels = async (request: Request, h: ResponseToolkit) => {
    const levels = await this.severityLevelService.getAll();

    return h.response({
      status: 'success',
      message: 'Successfully retrieve all available levels',
      data: {
        levels,
      },
    });
  };

  getSpecificLevel = async (request: Request, h: ResponseToolkit) => {
    const {level: levelParam} = request.params as {level: number};
    const level = await this.severityLevelService.getByLevel(levelParam);
    return h.response({
      status: 'success',
      message: 'Successfully retrieve specified level data',
      data: {
        level,
      },
    });
  };
}