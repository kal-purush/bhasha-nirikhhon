import got from 'got';
import cloneDeep from 'lodash.clonedeep';
import Metalsmith from 'metalsmith';

import PreviewCache from './cache/preview';
import { getFirstParentCommits } from './git';
import { NetlifyDeployData, netlifyDeploys } from './netlify';
import { OptionsInterface } from './plugin';
import { joinURL, MapWithDefault } from './utils';
import { debug } from './utils/log';
import { isFile, processFiles } from './utils/metalsmith';
import { PromiseValueType } from './utils/types';

const log = debug.extend('lookup');
const fileLog = log.extend('file');
const previewLog = log.extend('netlify-preview');

export interface FileDateStateInterface {
    published: DateState;
    modified: DateState;
}

export class DateState {
    private _datestr: string;
    private _isEstablished: boolean;

    public constructor(date: string | number | Date) {
        this._datestr =
            typeof date === 'string'
                ? date
                : (date instanceof Date ? date : new Date(date)).toISOString();
        this._isEstablished = false;
    }

    public get date(): string {
        return this._datestr;
    }
    public set date(datestr: string) {
        if (!this.established) {
            this._datestr = datestr;
        }
    }

    public get established(): boolean {
        return this._isEstablished;
    }
    public set established(isEstablished: boolean) {
        if (isEstablished) {
            this._isEstablished = true;
        }
    }

    public toString(): string {
        return this._datestr;
    }
}

export function isEstablished(dateState: FileDateStateInterface): boolean {
    return dateState.published.established && dateState.modified.established;
}

export function publishedDate(deploy: NetlifyDeployData): string {
    return deploy.published_at || deploy.created_at;
}

export async function getDeployList({
    siteID,
    accessToken,
}: {
    siteID: string;
    accessToken: string | null;
}): ReturnType<typeof netlifyDeploys> {
    const commitList = await getFirstParentCommits();
    log("got Git's commits hash");

    const deployList = await netlifyDeploys(siteID, {
        accessToken,
        commitHashList: commitList.map(commit => commit.hash),
    });
    log('fetched Netlify deploys');

    return deployList;
}

export async function fetchPage(
    url: string,
): Promise<got.Response<Buffer> | null> {
    try {
        const response = await got(url, { encoding: null });
        previewLog('%s / fetchd', url);
        return response;
    } catch (error) {
        if (error instanceof got.HTTPError) {
            previewLog('%s / fetchd', url);
            if (error.statusCode === 404) {
                return null;
            }
        }
        throw error;
    }
}

export async function fetchPageData({
    filename,
    urlpath,
    previewPageURL,
    deploy,
    pluginOptions,
}: {
    filename: string;
    urlpath: string;
    previewPageURL: string;
    deploy: NetlifyDeployData;
    pluginOptions: OptionsInterface;
}): Promise<
    | {
          filename: string;
          urlpath: string;
          previewPageURL: string;
          previewPageResponse: got.Response<Buffer>;
          contents: Buffer;
          metadata: { published: string; modified: string };
          previewPageNotFound: false;
      }
    | {
          filename: string;
          urlpath: string;
          previewPageURL: string;
          previewPageResponse: null;
          contents: null;
          previewPageNotFound: true;
      }
> {
    try {
        const previewPageResponse = await got(previewPageURL, {
            encoding: null,
        });
        previewLog('%s / fetchd', previewPageURL);

        const published = publishedDate(deploy);
        const modified = publishedDate(deploy);
        const contents = await pluginOptions.contentsConverter(
            previewPageResponse.body,
            { deploy, previewPageResponse, cachedResponse: null },
        );

        return {
            filename,
            urlpath,
            previewPageURL,
            previewPageResponse,
            contents,
            metadata: { published, modified },
            previewPageNotFound: false,
        };
    } catch (error) {
        if (error instanceof got.HTTPError) {
            previewLog('%s / fetchd', previewPageURL);

            if (error.statusCode === 404) {
                previewLog('%s / 404 Not Found', previewPageURL);

                return {
                    filename,
                    urlpath,
                    previewPageURL,
                    previewPageResponse: null,
                    contents: null,
                    previewPageNotFound: true,
                };
            }
        }
        throw error;
    }
}

export async function getPreviewDataList({
    targetFileList,
    files,
    dateStateMap,
    deploy,
    pluginOptions,
}: {
    targetFileList: readonly { filename: string; urlpath: string }[];
    files: Metalsmith.Files;
    dateStateMap: MapWithDefault<string, FileDateStateInterface>;
    deploy: NetlifyDeployData;
    pluginOptions: OptionsInterface;
}): Promise<{
    previewDataList: (PromiseValueType<ReturnType<typeof fetchPageData>>)[];
    files: Metalsmith.Files;
    dateStateMap: MapWithDefault<string, FileDateStateInterface>;
}> {
    const previewDataList = await Promise.all(
        targetFileList.map(async ({ filename, urlpath }) => {
            const dateState = dateStateMap.get(filename);

            const previewPageURL = joinURL(deploy.deployAbsoluteURL, urlpath);
            const previewData = await fetchPageData({
                filename,
                urlpath,
                previewPageURL,
                deploy,
                pluginOptions,
            });

            if (previewData.previewPageNotFound) {
                previewLog('%s / 404 Not Found', previewPageURL);

                if (!dateState.published.established) {
                    fileLog(
                        !dateState.modified.established
                            ? '%s / published date and modified date is established: %s / %s'
                            : '%s / published date is established: %s',
                        filename,
                        dateState.published,
                        dateState.modified,
                    );
                }

                delete files[filename];
                dateState.published.established = true;
                dateState.modified.established = true;
            } else {
                const fileData = files[filename];
                const { metadata } = previewData;

                fileData.published = new Date(metadata.published);
                fileData.modified = new Date(metadata.modified);

                if (!dateState.published.established) {
                    dateState.published.date = metadata.published;
                }
            }

            return previewData;
        }),
    );

    return {
        previewDataList,
        files,
        dateStateMap,
    };
}

export async function comparePages({
    previewDataList,
    processedFiles,
    dateStateMap,
    deploy,
    pluginOptions,
    metalsmith,
}: {
    previewDataList: PromiseValueType<
        ReturnType<typeof getPreviewDataList>
    >['previewDataList'];
    processedFiles: Metalsmith.Files;
    dateStateMap: MapWithDefault<string, FileDateStateInterface>;
    deploy: NetlifyDeployData;
    pluginOptions: OptionsInterface;
    metalsmith: Metalsmith;
}): Promise<{
    dateStateMap: MapWithDefault<string, FileDateStateInterface>;
}> {
    await Promise.all(
        previewDataList.map(async previewData => {
            if (previewData.previewPageNotFound) {
                return;
            }

            const {
                filename,
                previewPageURL,
                previewPageResponse,
                contents: previewPageContents,
                metadata,
            } = previewData;
            const dateState = dateStateMap.get(filename);
            const fileData: unknown = processedFiles[filename];

            if (dateState.modified.established) {
                return;
            }

            if (!isFile(fileData)) {
                fileLog(
                    '%s / contents was not generated. used metadata: %o',
                    filename,
                    {
                        ...metadata,
                        published: new Date(metadata.published),
                        modified: new Date(metadata.modified),
                    },
                );
                // TODO: processedFilesに対象のファイルのコンテンツが存在しなかった場合の処理
                return;
            }

            const fileContents = await pluginOptions.contentsConverter(
                fileData.contents,
                {
                    files: processedFiles,
                    filename,
                    fileData,
                    metalsmith,
                },
            );

            if (
                await pluginOptions.contentsEquals({
                    file: fileContents,
                    previewPage: previewPageContents,
                    metadata: {
                        deploy,
                        previewPageResponse,
                        cachedResponse: null,
                        files: processedFiles,
                        filename,
                        fileData,
                        metalsmith,
                    },
                })
            ) {
                fileLog(
                    '%s / matched the content of preview %s',
                    filename,
                    previewPageURL,
                );

                dateState.published.date = metadata.published;
            } else {
                fileLog(
                    '%s / did not match the content of preview %s',
                    filename,
                    previewPageURL,
                );

                dateState.modified.established = true;
                fileLog(
                    '%s / modified date is established: %s',
                    filename,
                    dateState.modified,
                );
            }
        }),
    );
    return { dateStateMap };
}

export default async function({
    targetFileList,
    options: pluginOptions,
    metalsmith,
    files,
    nowDate,
}: {
    targetFileList: readonly { filename: string; urlpath: string }[];
    options: OptionsInterface;
    metalsmith: Metalsmith;
    files: Metalsmith.Files;
    nowDate: number;
}): Promise<void> {
    /**
     * @see https://github.com/sounisi5011/sounisi5011.jp/issues/39#issuecomment-508548319
     */

    const cache = new PreviewCache(pluginOptions.cacheDir);
    const dateStateMap = new MapWithDefault<string, FileDateStateInterface>(
        () => ({
            published: new DateState(nowDate),
            modified: new DateState(nowDate),
        }),
    );

    for (const deploy of await getDeployList(pluginOptions)) {
        const {
            previewDataList,
            files: updatedFiles,
            dateStateMap: previewUpdatedDateStateMap,
        } = await getPreviewDataList({
            targetFileList,
            files: cloneDeep(files),
            dateStateMap,
            deploy,
            pluginOptions,
        });

        const processedFiles = await processFiles(
            metalsmith,
            updatedFiles,
            pluginOptions.plugins,
        );

        const { dateStateMap: compareUpdatedDateStateMap } = await comparePages(
            {
                previewDataList,
                processedFiles,
                dateStateMap: previewUpdatedDateStateMap,
                deploy,
                pluginOptions,
                metalsmith,
            },
        );

        const isAllfileEstablished = [
            ...compareUpdatedDateStateMap.values(),
        ].every(isEstablished);
        if (isAllfileEstablished) {
            break;
        }
    }

    cache.save();
}