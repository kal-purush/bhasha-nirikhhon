import { connectionActions } from '../../actions';
import { connectionConstants } from '../../constants';
import { User, IAction, Device, DeviceCategory, AccessType } from '../../models';
import { ConnectionService } from '../../services';
import { Dispatch } from 'redux';

describe('Connection actions', () =>
{
    const user: User = {
        email: 'test@gmail.com',
        password: '1234567',
        firstname: 'chris',
        lastname: 'h',
    };

    const device: Device = {
        name: "Bar",
        owner: user,
        code: "1234",
        contractURL: "http://hello.com",
        accessType: AccessType.PRIVATE,
        deviceCategory: DeviceCategory.FOOD_AND_DRINK
    }

    const mockDispatch: Dispatch<any> = jest.fn();

    it('should get a connection', async () =>
    {
        ConnectionService.get = jest.fn((code: string) => Promise.resolve(device));
        const res: any = await connectionActions.get('1234')(mockDispatch);

        expect(ConnectionService.get).toBeCalled();
        expect(mockDispatch).toBeCalledWith(<IAction> {
            type: connectionConstants.GET_CONNECTION_SUCCESS,
            searchedDevice: device
        });
    });

    it('should have error getting connection', async () =>
    {
        ConnectionService.get = jest.fn((code: string) => Promise.reject("ERR"));
        const res: any = await connectionActions.get('1234')(mockDispatch);

        expect(ConnectionService.get).toBeCalled();
        expect(mockDispatch).toBeCalledWith(<IAction> {
            type: connectionConstants.GET_CONNECTION_ERROR,
            error: "ERR"
        });
    });

    it('should get public device array', async () =>
    {
        ConnectionService.getPublicDevices = jest.fn(() => Promise.resolve(new Array<Device>()));
        const res: any = await connectionActions.getPublic()(mockDispatch);

        expect(ConnectionService.getPublicDevices).toBeCalled();
        expect(mockDispatch).toBeCalledWith(<IAction> {
            type: connectionConstants.GET_PUBLIC_DEVICES_SUCCESS,
            publicDevices: []
        });
    });

    it('should have error getting public devices', async () =>
    {
        ConnectionService.getPublicDevices = jest.fn(() => Promise.reject("ERR"));
        const res: any = await connectionActions.getPublic()(mockDispatch);

        expect(ConnectionService.getPublicDevices).toBeCalled();
        expect(mockDispatch).toBeCalledWith(<IAction> {
            type: connectionConstants.GET_PUBLIC_DEVICES_ERROR,
            error: "ERR"
        });
    });

    it('should connect', async () =>
    {
        const res: any = await connectionActions.connect(device);

        expect(res.device).toBe(device);
        expect(res.type).toBe(connectionConstants.CONNECT_SUCCESS);
    });
});