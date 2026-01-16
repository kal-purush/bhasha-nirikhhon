import { DataSource } from "typeorm";
import { Status } from "../../utils/status.enum";
import StatusService from '../../service/status.service'
import StatusRepository from '../../repository/status.repository'

describe('Role Service', () => {

    let statusService;
    let statusRepository;

    beforeAll(() => {

        statusRepository = new StatusRepository()
        statusService = new StatusService(statusRepository);
    })

    test('Test Status', async () => {
        const role = await statusService.getStatus();
        expect(role).toStrictEqual(Object.keys(Status))
    })
})