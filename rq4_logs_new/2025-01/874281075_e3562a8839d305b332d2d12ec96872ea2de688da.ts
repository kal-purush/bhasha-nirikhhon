import { IsNotEmpty, IsString } from 'class-validator';

class CreateAvatar {
  @IsString()
  @IsNotEmpty()
  url: string;

  @IsNotEmpty()
  @IsString()
  assetId: string;

  @IsNotEmpty()
  @IsString()
  publicId: string;
}

export default CreateAvatar;