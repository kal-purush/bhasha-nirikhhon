
import { ApiProperty } from "@nestjs/swagger";
import { Expose } from "class-transformer";
import { IsBoolean, IsEnum, IsOptional, IsString, IsUrl } from "class-validator";


export enum TileSourceEnum {
  XYZ = "XYZ",
  WMTS = "WMTS"
}

export class LayersConfigDto {

  @ApiProperty({
    description: 'The name of the layer',
    type: String,
    required: true
  })
  @IsString()
  @Expose()
  layerName: string;

  @ApiProperty({
    description: 'The source of the tile layer',
    enum: TileSourceEnum,
    required: false
  })
  @IsOptional()
  @IsEnum(TileSourceEnum)
  @Expose()
  tileLayerSource?: TileSourceEnum;

  @ApiProperty({
    description: 'URL for XYZ tiles',
    type: String,
    required: false,
  })
  @IsOptional()
  @IsUrl()
  @Expose()
  XYZtilesURL?: string;

  @ApiProperty({
    description: 'URL for capabilities',
    type: String,
    required: false,
  })
  @IsOptional()
  @IsUrl()
  @Expose()
  capabilitiesUrl?: string;

  @ApiProperty({
    description: 'URL for capabilities',
    type: String,
    required: false,
  })
  @IsOptional()
  @IsUrl()
  @Expose()
  capabilitiesLayerName?: string;
  
  @ApiProperty({
    description: 'Flag to indicate if the layer should be deleted',
    type: Boolean,
    required: false,
  })
  @IsBoolean()
  @IsOptional()
  @Expose()
  delete?: boolean;

  toString() {
    return JSON.stringify(this)
  }

}