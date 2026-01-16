import {
  Controller,
  Post,
  Delete,
  UploadedFiles,
  UseInterceptors,
  Param,
  UseGuards,
} from '@nestjs/common';
import { FilesInterceptor } from '@nestjs/platform-express';
import { FileService } from './file.service';
import { JwtAuthGuard } from 'src/auth/jwt-auth.guard';

//postman으로 파일과 json 콘텐츠를 동시에 보낼 수 없어 file upload api는 따로 나눔
@Controller('files')
export class FileController {
  constructor(private readonly fileService: FileService) {}

  @UseGuards(JwtAuthGuard)
  @Post('/images/:id')
  @UseInterceptors(FilesInterceptor('file'))
  uploadFile(
    @Param('id') postId: number,
    @UploadedFiles() files: Express.MulterS3.File[],
  ) {
    return this.fileService.uploadFile(postId, files);
  }

  @UseGuards(JwtAuthGuard)
  @Delete('/images/:id')
  DeleteFile(@Param('id') postId: number) {
    return this.fileService.deleteFile(postId);
  }
}