import * as exec from '@actions/exec';

export interface S3UpdateOptions {
  deployPath: string,
  version?: string,
  branch?: string,
  bucket: string,
  prefix: string,
  topBranchesJSON?: string
}

// Upload dist folder to the S3 bucket with the prefix followed by the deployPath
export async function s3Update(options: S3UpdateOptions): Promise<void> {
  // Currently this syncs the non index.html files first and then updates the index.html
  // files. So there is a moment when index.html files do not match the resources.
  // We could do this in 3 steps so there was no time when the index.html files
  // referenced missing resources. But even in that case a browser could have fetched
  // the old index.html just before it was replaced and then would be trying to
  // fetch the old resources as they are being deleted. The safest approach would be
  // to queue some kind of cleanup task which would delete the old resources several
  // minutes later.
  // However, branches are not intended for production use, so the occational broken
  // branch does not seem worth fixing.

  const { deployPath, version, branch, bucket, prefix, topBranchesJSON } = options;

  const topLevelS3Url = `s3://${bucket}/${prefix}`;
  const deployS3Url = `${topLevelS3Url}/${deployPath}`;
  const maxAge = version ? 60*60*24*365 : 60*2;


  const excludes = `--exclude "index.html" --exclude "index-top.html"`;
  const cacheControl = `--cache-control "max-age=${maxAge}"`;
  await exec.exec(`aws s3 sync ./dist ${deployS3Url} --delete ${excludes} ${cacheControl}`);

  const noCache = `--cache-control "no-cache, max-age=0"`;
  await exec.exec(`aws s3 cp ./dist/index.html ${deployS3Url}/ ${noCache}`);
  await exec.exec(`aws s3 cp ./dist/index-top.html ${deployS3Url}/ ${noCache}`);

  if (topBranchesJSON) {
    const topBranches = JSON.parse(topBranchesJSON);
    if (topBranches.includes(branch)) {
      await exec.exec(`aws s3 cp ${deployS3Url}/index-top.html ${topLevelS3Url}/index-${branch}.html`);
    }
  }

}