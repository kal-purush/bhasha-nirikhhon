import { Injectable, Logger } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class VideoChunksService {
  private readonly logger = new Logger(VideoChunksService.name);
  private readonly tempDir: string;

  constructor(private configService: ConfigService) {
    // Configure a temp directory for chunks
    this.tempDir = path.join(process.cwd(), 'temp');
    this.ensureTempDirExists();
  }

  private async ensureTempDirExists(): Promise<void> {
    try {
      await fs.promises.access(this.tempDir);
      this.logger.log(`Using existing temp directory at ${this.tempDir}`);
    } catch (error) {
      this.logger.error(error.message);
      await fs.promises.mkdir(this.tempDir, { recursive: true });
      this.logger.log(`Created temp directory at ${this.tempDir}`);
    }
  }

  async saveChunk(
    fileId: string,
    chunkIndex: number,
    totalChunks: number,
    chunkBuffer: Buffer,
  ): Promise<void> {
    const chunkDir = path.join(this.tempDir, fileId);

    try {
      await fs.promises.access(chunkDir);
    } catch (error) {
      this.logger.error(error.message);
      await fs.promises.mkdir(chunkDir, { recursive: true });
      this.logger.log(`Created directory for file ${fileId}`);
    }

    const chunkPath = path.join(chunkDir, `${chunkIndex}`);

    try {
      await fs.promises.writeFile(chunkPath, chunkBuffer);
      this.logger.log(
        `Saved chunk ${chunkIndex + 1}/${totalChunks} for ${fileId}`,
      );
    } catch (error) {
      this.logger.error(
        `Error saving chunk ${chunkIndex} for ${fileId}`,
        error,
      );
      throw new Error(
        `Failed to save chunk ${chunkIndex} for ${fileId}: ${error.message}`,
      );
    }
  }

  async combineChunks(fileId: string, totalChunks: number): Promise<Buffer> {
    const chunkDir = path.join(this.tempDir, fileId);
    const buffers: Buffer[] = [];

    this.logger.log(`Combining ${totalChunks} chunks for ${fileId}`);

    try {
      // First, verify that all chunks exist
      for (let i = 0; i < totalChunks; i++) {
        const chunkPath = path.join(chunkDir, `${i}`);
        try {
          await fs.promises.access(chunkPath);
        } catch (error) {
          this.logger.error(error.message);
          throw new Error(`Missing chunk ${i} for file ${fileId}`);
        }
      }

      // Then read and combine them
      for (let i = 0; i < totalChunks; i++) {
        const chunkPath = path.join(chunkDir, `${i}`);
        const chunkBuffer = await fs.promises.readFile(chunkPath);
        buffers.push(chunkBuffer);
        this.logger.verbose(
          `Added chunk ${i} (${chunkBuffer.length} bytes) to combined buffer`,
        );
      }

      const combinedBuffer = Buffer.concat(buffers);
      this.logger.log(
        `Successfully combined ${totalChunks} chunks for ${fileId}, total size: ${combinedBuffer.length} bytes`,
      );

      // Clean up chunks in the background
      this.cleanupChunks(fileId).catch((err) => {
        this.logger.error(`Failed to cleanup chunks for ${fileId}`, err);
      });

      return combinedBuffer;
    } catch (error) {
      this.logger.error(`Error combining chunks for ${fileId}`, error);
      throw error;
    }
  }

  private async cleanupChunks(fileId: string): Promise<void> {
    const chunkDir = path.join(this.tempDir, fileId);

    try {
      this.logger.log(`Cleaning up chunks for ${fileId}`);
      const files = await fs.promises.readdir(chunkDir);

      // Delete each chunk file
      for (const file of files) {
        await fs.promises.unlink(path.join(chunkDir, file));
      }

      // Remove the directory
      await fs.promises.rmdir(chunkDir);
      this.logger.log(`Successfully cleaned up all chunks for ${fileId}`);
    } catch (error) {
      this.logger.error(`Error cleaning up chunks for ${fileId}`, error);
      // Don't rethrow this error since cleanup is non-critical
    }
  }

  async getUploadStatus(
    fileId: string,
    totalChunks: number,
  ): Promise<{
    uploaded: number;
    total: number;
    complete: boolean;
    missingChunks?: number[];
  }> {
    const chunkDir = path.join(this.tempDir, fileId);

    try {
      await fs.promises.access(chunkDir);
      const files = await fs.promises.readdir(chunkDir);

      // Determine which chunks are present
      const uploadedChunks = new Set(
        files
          .map((filename) => parseInt(filename, 10))
          .filter(
            (index) => !isNaN(index) && index >= 0 && index < totalChunks,
          ),
      );

      // Determine which chunks are missing
      const missingChunks: number[] = [];
      for (let i = 0; i < totalChunks; i++) {
        if (!uploadedChunks.has(i)) {
          missingChunks.push(i);
        }
      }

      const complete = uploadedChunks.size === totalChunks;

      return {
        uploaded: uploadedChunks.size,
        total: totalChunks,
        complete,
        missingChunks: missingChunks.length > 0 ? missingChunks : undefined,
      };
    } catch (error) {
      this.logger.error(error.message);
      // Directory doesn't exist, so no chunks uploaded yet
      this.logger.log(`No chunks found for ${fileId}`);
      return {
        uploaded: 0,
        total: totalChunks,
        complete: false,
        missingChunks: Array.from({ length: totalChunks }, (_, i) => i),
      };
    }
  }

  async cleanupIncompleteUploads(olderThanHours: number = 24): Promise<void> {
    try {
      const entries = await fs.promises.readdir(this.tempDir, {
        withFileTypes: true,
      });
      const cutoffTime = Date.now() - olderThanHours * 60 * 60 * 1000;

      for (const entry of entries) {
        if (entry.isDirectory()) {
          const dirPath = path.join(this.tempDir, entry.name);
          const stats = await fs.promises.stat(dirPath);

          if (stats.mtimeMs < cutoffTime) {
            this.logger.log(`Cleaning up incomplete upload: ${entry.name}`);
            await this.cleanupChunks(entry.name);
          }
        }
      }

      this.logger.log(
        `Finished cleanup of incomplete uploads older than ${olderThanHours} hours`,
      );
    } catch (error) {
      this.logger.error('Error during cleanup of incomplete uploads', error);
    }
  }
}