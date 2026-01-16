import { assertEquals } from "@std/assert/equals";
import { Logger } from "@/logger.ts";
import { CdInfoList } from "@/model/cd_info_list.ts";
import { assertThrows } from "@std/assert/throws";
import {
  getCdInfoListCustomJson,
  getCdInfoListFromGithub,
} from "@/functions/data_json_convertor.ts";

Deno.test("並び替えテスト：タイトル（昇順）", async () => {
  const source: CdInfoList = await getCdInfoListCustomJson(
    createLocalJsonUrl("../resources/sort/title_test_source.json"),
  );

  const sort = "title:asc";
  const expected = JSON.stringify(
    await getCdInfoListCustomJson(
      createLocalJsonUrl("../resources/sort/title_asc_test_expected.json"),
    ).then((cdInfoList: CdInfoList) => cdInfoList.getList()),
  );

  const actual = JSON.stringify(source.sort(sort).getList());

  Logger.info(`ソート指定：${sort}`);
  Logger.info(`ソート結果：${actual} / 期待値：${expected}`);

  assertEquals(actual, expected);
});

Deno.test("並び替えテスト：タイトル（降順）", async () => {
  const source: CdInfoList = await getCdInfoListCustomJson(
    createLocalJsonUrl("../resources/sort/title_test_source.json"),
  );

  const sort = "title:desc";
  const expected = JSON.stringify(
    await getCdInfoListCustomJson(
      createLocalJsonUrl("../resources/sort/title_desc_test_expected.json"),
    ).then((cdInfoList: CdInfoList) => cdInfoList.getList()),
  );

  const actual = JSON.stringify(source.sort(sort).getList());

  Logger.info(`ソート指定：${sort}`);
  Logger.info(`ソート結果：${actual} / 期待値：${expected}`);

  assertEquals(actual, expected);
});

Deno.test("並び替えテスト：品番（昇順）", async () => {
  const source: CdInfoList = await getCdInfoListCustomJson(
    createLocalJsonUrl("../resources/sort/recordNumbers_test_source.json"),
  );

  const sort = "recordNumbers:asc";
  const expected = JSON.stringify(
    await getCdInfoListCustomJson(
      createLocalJsonUrl(
        "../resources/sort/recordNumbers_asc_test_expected.json",
      ),
    ).then((cdInfoList: CdInfoList) => cdInfoList.getList()),
  );

  const actual = JSON.stringify(source.sort(sort).getList());

  Logger.info(`ソート指定：${sort}`);
  Logger.info(`ソート結果：${actual} / 期待値：${expected}`);

  assertEquals(actual, expected);
});

Deno.test("並び替えテスト：品番（降順）", async () => {
  const source: CdInfoList = await getCdInfoListCustomJson(
    createLocalJsonUrl("../resources/sort/recordNumbers_test_source.json"),
  );

  const sort = "recordNumbers:desc";
  const expected = JSON.stringify(
    await getCdInfoListCustomJson(
      createLocalJsonUrl(
        "../resources/sort/recordNumbers_desc_test_expected.json",
      ),
    ).then((cdInfoList: CdInfoList) => cdInfoList.getList()),
  );

  const actual = JSON.stringify(source.sort(sort).getList());

  Logger.info(`ソート指定：${sort}`);
  Logger.info(`ソート結果：${actual} / 期待値：${expected}`);

  assertEquals(actual, expected);
});

Deno.test("並び替えテスト：リリース日（昇順）", async () => {
  const source: CdInfoList = await getCdInfoListCustomJson(
    createLocalJsonUrl("../resources/sort/releaseDate_test_source.json"),
  );

  const sort = "releaseDate:asc";
  const expected = JSON.stringify(
    await getCdInfoListCustomJson(
      createLocalJsonUrl(
        "../resources/sort/releaseDate_asc_test_expected.json",
      ),
    ).then((cdInfoList: CdInfoList) => cdInfoList.getList()),
  );

  const actual = JSON.stringify(source.sort(sort).getList());

  Logger.info(`ソート指定：${sort}`);
  Logger.info(`ソート結果：${actual} / 期待値：${expected}`);

  assertEquals(actual, expected);
});

Deno.test("並び替えテスト：リリース日（降順）", async () => {
  const source: CdInfoList = await getCdInfoListCustomJson(
    createLocalJsonUrl("../resources/sort/releaseDate_test_source.json"),
  );

  const sort = "releaseDate:desc";
  const expected = JSON.stringify(
    await getCdInfoListCustomJson(
      createLocalJsonUrl(
        "../resources/sort/releaseDate_desc_test_expected.json",
      ),
    ).then((cdInfoList: CdInfoList) => cdInfoList.getList()),
  );

  const actual = JSON.stringify(source.sort(sort).getList());

  Logger.info(`ソート指定：${sort}`);
  Logger.info(`ソート結果：${actual} / 期待値：${expected}`);

  assertEquals(actual, expected);
});

Deno.test("並び替えテスト：アーティスト名（昇順）", async () => {
  const source: CdInfoList = await getCdInfoListCustomJson(
    createLocalJsonUrl("../resources/sort/artist_test_source.json"),
  );

  const sort = "artist:asc";
  const expected = JSON.stringify(
    await getCdInfoListCustomJson(
      createLocalJsonUrl(
        "../resources/sort/artist_asc_test_expected.json",
      ),
    ).then((cdInfoList: CdInfoList) => cdInfoList.getList()),
  );

  const actual = JSON.stringify(source.sort(sort).getList());

  Logger.info(`ソート指定：${sort}`);
  Logger.info(`ソート結果：${actual} / 期待値：${expected}`);

  assertEquals(actual, expected);
});

Deno.test("並び替えテスト：アーティスト名（降順）", async () => {
  const source: CdInfoList = await getCdInfoListCustomJson(
    createLocalJsonUrl("../resources/sort/artist_test_source.json"),
  );

  const sort = "artist:desc";
  const expected = JSON.stringify(
    await getCdInfoListCustomJson(
      createLocalJsonUrl(
        "../resources/sort/artist_desc_test_expected.json",
      ),
    ).then((cdInfoList: CdInfoList) => cdInfoList.getList()),
  );

  const actual = JSON.stringify(source.sort(sort).getList());

  Logger.info(`ソート指定：${sort}`);
  Logger.info(`ソート結果：${actual} / 期待値：${expected}`);

  assertEquals(actual, expected);
});

Deno.test("並び替えテスト：不明パラメータ指定（ソート形式指定）", async () => {
  const source: CdInfoList = await getCdInfoListFromGithub();

  const sort = "ABCD:asc";
  const expected = JSON.stringify(source.getList());

  const actual = JSON.stringify(source.sort(sort).getList());

  assertEquals(actual, expected);
});

Deno.test("並び替えテスト：不明パラメータ指定（ソート形式未指定）", async () => {
  const source: CdInfoList = await getCdInfoListFromGithub();

  const sort = "ABCD";

  assertThrows(
    () => {
      return JSON.stringify(source.sort(sort).getList());
    },
    Error,
    "Invalid sort order",
  );
});

Deno.test("並び替えテスト：不明パラメータ指定（カンマだけ）", async () => {
  const source: CdInfoList = await getCdInfoListFromGithub();

  const sort = ",";

  assertThrows(
    () => {
      return JSON.stringify(source.sort(sort).getList());
    },
    Error,
    "Invalid sort order",
  );
});

/**
 * ローカルファイルの相対パスを絶対ファイルURLに変換する
 * @param relativePath ローカルファイルの相対パス
 * @returns 変換後の絶対ファイルURL
 */
function createLocalJsonUrl(relativePath: string): string {
  return new URL(relativePath, import.meta.url).toString();
}