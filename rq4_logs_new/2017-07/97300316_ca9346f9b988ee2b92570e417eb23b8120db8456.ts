import { CleanLocales } from './../use-cases/clean-locales/CleanLocales';
import { Command } from './../use-cases/clean-locales/Command';
import { Responder } from './../use-cases/clean-locales/Responder';
import { FileSystemService } from './../services/file-system/FileSystemService';
import { CleaningService } from './../services/cleaning/CleaningService';

export function cleanLocales(directory: string, options: any) {
  directory = directory || './';
  const fillMissing = options.fillMissing || false;
  const sort = options.sort || false;
  const save = options.save || false;

  const cleanLocales = new CleanLocales(new FileSystemService(), new CleaningService());
  const command = new Command(directory, fillMissing, sort, save);

  cleanLocales.execute(command, <Responder> {
    localesCleaned() {
      console.log('cleaned...');
    },

    cannotGetLocales(e) {
      console.log('canot get locales');
    },

    cannotSaveLocales(e) {
      console.log('cannot save locles.');
    }
  });
};