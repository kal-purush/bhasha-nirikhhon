/// <reference path="type_declarations/index.d.ts" />
import {tmpdir} from 'os';
import fs = require('fs');
import {join} from 'path';
// import {extname} from 'path';
import {exec} from 'child_process';
import {logger} from 'loge';

interface ConvertOptions {
  quality: number | string;
  resize?: string;
}

/**
Read image at `input_filepath` and write compressed JPEG to `output_filepath`.

It will clobber the file at `output_filepath` if it exists.

- `options.resize` is optional.
- `options.quality` is required

*/
export function convert(input_filepath: string,
                        output_filepath: string,
                        options: ConvertOptions,
                        callback: (error?: Error) => void) {
  var resize_args = options.resize ? `-resize ${options.resize}` : '';
  var convert_command = `convert "${input_filepath}" ${resize_args} TGA:-`;
  var cjpeg_command = `cjpeg -quality ${options.quality} -outfile "${output_filepath}" -targa`;
  logger.debug(`$ ${convert_command} | ${cjpeg_command}`);
  exec(`${convert_command} | ${cjpeg_command}`, (err, stdout, stderr) => {
    if (err) {
      logger.error('stdout: %s; stderr: %s', stdout, stderr);
      return callback(err);
    }

    if (stdout) logger.info(`stdout: ${stdout}`);
    if (stderr) logger.debug(`stderr: ${stderr}`);
    callback();
  });
}

/**
`dirname` should not end with a slash.
`basename` should contain no slashes.
`extname` should start with '.' if not empty.
*/
class SystemFile {
  constructor(public dirpath: string,
              public basename: string,
              public extname: string = '') { }
  replace({dirpath, basename, extname}: {dirpath?: string, basename?: string, extname?: string}): SystemFile {
    return new SystemFile(dirpath || this.dirpath, basename || this.basename, extname || this.extname);
  }
  toString(): string {
    return this.dirpath + '/' + this.basename + this.extname;
  }
  static parse(filepath: string): SystemFile {
    var match = filepath.match(/^(.+\/)?([^/]+?)(\.[^.]+)$/);
    if (match === null) {
      throw new Error(`SystemFile cannot parse filepath: "${filepath}"`);
    }
    return new SystemFile((match[1] || './').replace(/\/$/, ''), match[2], match[3]);
  }
}

export function convertWithBackup(input_filepath: string,
                                  options: ConvertOptions,
                                  callback: (error?: Error) => void) {
  var input_file = SystemFile.parse(input_filepath);
  var input_stats = fs.statSync(input_file.toString());

  var backup_file = input_file.replace({extname: '.bak' + input_file.extname});
  // if the backup file already exists, return immediately
  var backup_exists = fs.existsSync(backup_file.toString());
  if (backup_exists) {
    logger.error(`${backup_file.toString()} already exists; not converting ${input_filepath}`);
    return callback();
  }

  var temp_directory = tmpdir();
  var temp_filepath = join(temp_directory, input_file.basename + input_file.extname);
  // temp_directory will be an existing directory, something like:
  //     /var/folders/m8/cq7z9jxj0774qz_3yg0kw5k40000gn/T/
  // var temp_filepath = temp.path({suffix: input_extname});
  logger.debug(`converting "${input_filepath}" (using tempfile "${temp_filepath}")`);
  convert(input_filepath, temp_filepath, options, error => {
    if (error) return callback(error);

    // backup original
    logger.debug(`moving "${input_filepath}" to "${backup_file.toString()}"`);
    fs.linkSync(input_filepath, backup_file.toString());
    // remove original
    fs.unlinkSync(input_filepath);
    // move tempfile into original's place
    logger.debug(`copying tempfile "${temp_filepath}" to "${input_filepath}"`);
    fs.linkSync(temp_filepath, input_filepath);

    var converted_stats = fs.statSync(input_filepath);
    logger.info(`recompressed version of ${input_filepath} is ${(100.0 * converted_stats.size / input_stats.size).toFixed(2)}% the size of the original`);

    callback();
  });
}