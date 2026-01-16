import { Injectable } from '@nestjs/common';
import { TestEntryModel } from '../../../../models/regression-tests/test-entry.models';

@Injectable()
export class TestEntryService {
  async getAll() {
    return await TestEntryModel.find({});
  }

  async create(category: string, new_command: string, files: Buffer[]) {
    try {
      return await TestEntryModel.create({
        command: new_command,
        category: category,
        correctFiles: files,
      });
    } catch (error) {
      console.error('Error while creating the test entry');
      // tslint:disable-next-line: no-console
      console.debug(error.stack);
      throw error;
    }
  }
}