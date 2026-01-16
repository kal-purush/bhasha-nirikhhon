import * as BrowserFS from 'browserfs';
import explode from './explode';
import unshrink from './unshrink';
import unreduce from './unreduce';
const ZipFS = BrowserFS.FileSystem.ZipFS;
const CompressionMethod = ZipFS.CompressionMethod;
const Buffer = BrowserFS.BFSRequire('buffer').Buffer;
const Errors = BrowserFS.Errors;
ZipFS.RegisterDecompressionMethod(CompressionMethod.IMPLODE, (data, compressedSize, uncompressedSize, flags) => {
  const output = new Buffer(uncompressedSize);
  const success = explode(flags, data.subarray(0, compressedSize), output, uncompressedSize);
  // Returns 5 when it reads one byte too many.
  // It's OK to ignore.
  if (success === 0 || success === 5) {
    return output;
  } else {
    throw new Errors.ApiError(Errors.ErrorCode.EIO, `Decompression failed.`);
  }
});

ZipFS.RegisterDecompressionMethod(CompressionMethod.SHRUNK, (data, compressedSize, uncompressedSize) => {
  const output = new Buffer(uncompressedSize);
  const success = unshrink(data.subarray(0, compressedSize), output, uncompressedSize);
  if (success === 0) {
    return output;
  } else {
    throw new Errors.ApiError(Errors.ErrorCode.EIO, `Decompression failed.`);
  }
});

function unreduceMethod(data: Buffer, compressedSize: number, uncompressedSize: number, level: number): Buffer {
  const output = new Buffer(uncompressedSize);
  const success = unreduce(data.subarray(0, compressedSize), compressedSize, output, uncompressedSize, level);
  if (success === uncompressedSize) {
    return output;
  } else {
    throw new Errors.ApiError(Errors.ErrorCode.EIO, `Decompression failed.`);
  }
}

ZipFS.RegisterDecompressionMethod(CompressionMethod.REDUCED_1, (d, c, u) => unreduceMethod(d, c, u, 1));
ZipFS.RegisterDecompressionMethod(CompressionMethod.REDUCED_2, (d, c, u) => unreduceMethod(d, c, u, 2));
ZipFS.RegisterDecompressionMethod(CompressionMethod.REDUCED_3, (d, c, u) => unreduceMethod(d, c, u, 3));
ZipFS.RegisterDecompressionMethod(CompressionMethod.REDUCED_4, (d, c, u) => unreduceMethod(d, c, u, 4));