import { ApiModelProperty } from '@nestjs/swagger';

export class JoinedCourseDataDto {
    @ApiModelProperty({ type: Number, required: true })
    public joinedCourseId: number;
}