import IUserDTO from "@modules/users/dtos/IUserDTO";

export default interface IProviderDTO extends IUserDTO{
    address: string;
    description: string;
}