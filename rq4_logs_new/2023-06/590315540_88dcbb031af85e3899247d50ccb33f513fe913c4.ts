import { MANIFEST_DOWNLOAD_FORMAT } from "../../../apis/azul/common/entities";

/**
 * Bulk download execution environment.
 */
export type BulkDownloadExecutionEnvironment =
  BULK_DOWNLOAD_EXECUTION_ENVIRONMENT;

/**
 * Set of supported shells that bulk download curl can be executed on.
 */
export enum BULK_DOWNLOAD_EXECUTION_ENVIRONMENT {
  "BASH" = "bash",
  "CMD_EXE" = "cmd.exe",
}

/**
 * Manifest download format.
 */
export type ManifestDownloadFormat = MANIFEST_DOWNLOAD_FORMAT;

/**
 * Hook returning file manifest request parameters.
 */
export type UseExportParams = (
  manifestFormat: ManifestDownloadFormat
) => URLSearchParams;

/**
 * Hook returning file manifest request URL.
 */
export type UseExportRequestURL = (requestParams: URLSearchParams) => string;