import { ApiModelProperty } from '@nestjs/swagger';

import { IFile } from '../../models/common.models';

export class FileDto implements IFile {
    @ApiModelProperty({ type: Number, required: true })
    public id: number;

    @ApiModelProperty({ type: String, required: true })
    public path: string;

    @ApiModelProperty({ type: String, required: true })
    public name: string;

    @ApiModelProperty({ type: String, required: true })
    public originalName: string;

    @ApiModelProperty({ type: String, required: true })
    public mimeType: string;

    @ApiModelProperty({ type: Number, required: true })
    public size: number;

    @ApiModelProperty({ type: String, required: true })
    public addedAt: string;
}