package jp.co.example.ecommerce_b.service;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import jp.co.example.ecommerce_b.domain.Favorite;

@SpringBootTest
class FavoriteServiceTest {

	@Autowired
	private FavoriteService service;

	private static final RowMapper<Favorite> FAVORITE_ROW_MAPPER = new BeanPropertyRowMapper<>(Favorite.class);

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		SingleConnectionDataSource singleConnectionDataSource = new SingleConnectionDataSource(
				"jdbc:postgresql://localhost:5432/ec_exam_test", "postgres", "postgres", true);
		NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(singleConnectionDataSource);
		String sql = "DELETE FROM favorites";
		template.update(sql, (SqlParameterSource) (null));
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	@DisplayName("商品をお気に入り登録するテスト")
	void testInsert() {
		// テスト用のデータをinsert
		Favorite insertFavorite = new Favorite();
//		insertFavorite.setId(1);
		insertFavorite.setUserId(1);
		insertFavorite.setItemId(1);
		Timestamp nowDate = new Timestamp(System.currentTimeMillis());// 現在の時刻データを代入
		insertFavorite.setFavoriteDate(nowDate);
		service.insertFavorite(insertFavorite);// insert実行

		// 正常にinsertできたかsqlを発行して確認
		SingleConnectionDataSource singleConnectionDataSource = new SingleConnectionDataSource(
				"jdbc:postgresql://localhost:5432/ec_exam_test", "postgres", "postgres", true);
		NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(singleConnectionDataSource);
		String sql = "SELECT * FROM favorites WHERE user_id = 1 AND item_id = 1;";
		Favorite favorite = template.queryForObject(sql, (SqlParameterSource) (null), FAVORITE_ROW_MAPPER);
		assertNotNull(favorite);

		// 日付の型をTimeStampからStringに変換
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");// 指定した文字列形式を代入
		String str = format.format(nowDate);// formatで指定した形式に変換
//		System.out.println(str);
		// 取得データも同様に変換しておく
		String str2 = format.format(favorite.getFavoriteDate());

		// 取得した情報をassertで確認
		assertEquals(insertFavorite.getUserId(), favorite.getUserId());
		assertEquals(insertFavorite.getItemId(), favorite.getItemId());
		assertEquals(str, str2);
	}

	@Test
	@DisplayName("UserIdとItemIdからお気に入り登録情報を取得する")
	void testFavoriteItemByUserId() {
		// テスト用のデータをinsert
		SingleConnectionDataSource singleConnectionDataSource = new SingleConnectionDataSource(
				"jdbc:postgresql://localhost:5432/ec_exam_test", "postgres", "postgres", true);
		NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(singleConnectionDataSource);
		String insertSql = "INSERT INTO favorites(id, user_id, item_id, favorite_date)"
				+ " VALUES('2', '2', '2', '2022-07-26')";
		template.update(insertSql, (SqlParameterSource) (null));

		// 正常にinsertできたかsqlを発行して確認
		String sql = "SELECT * FROM favorites WHERE user_id = 2 AND item_id = 2;";
		Favorite favorite = template.queryForObject(sql, (SqlParameterSource) (null), FAVORITE_ROW_MAPPER);// ここで情報を取得しておく
		assertNotNull(favorite);

		// 日付の型をTimeStampからStringに変換
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");// 指定した文字列形式を代入
		String str = format.format(favorite.getFavoriteDate());// DBから取得した日付データをformatで指定した形式に変換

		// UserIdとItemIdからお気に入り情報を取得する
		Favorite findFavorite = service.findByUserIdItemId(favorite.getUserId(), favorite.getItemId());
		String str2 = format.format(findFavorite.getFavoriteDate());// DBから取得した日付データをformatで指定した形式に変換

		// 取得した情報をassertで確認
		assertEquals(favorite.getUserId(), findFavorite.getUserId());
		assertEquals(favorite.getItemId(), findFavorite.getItemId());
		assertEquals(str, str2);
	}

	@Test
	@DisplayName("ユーザーのお気に入りリストを取得するテスト")
	void testFavoriteAll() {
		// テスト用のデータを複数件insert
		SingleConnectionDataSource singleConnectionDataSource = new SingleConnectionDataSource(
				"jdbc:postgresql://localhost:5432/ec_exam_test", "postgres", "postgres", true);
		NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(singleConnectionDataSource);
		String insertSql = "INSERT INTO favorites(id, user_id, item_id, favorite_date)"
				+ " VALUES('3', '3', '3', '2022-07-26')" + " ,('4', '3', '4', '2022-07-26')";
		SqlParameterSource param = new MapSqlParameterSource();
		template.update(insertSql, param);

		// 正常にinsertできたかsqlを発行して確認
		String sql = "SELECT * FROM favorites WHERE user_id = 3;";
		List<Favorite> insertFavoriteList = template.query(sql, param, FAVORITE_ROW_MAPPER);// ここで情報を取得しておく
		assertNotNull(insertFavoriteList);

		// データ数を取得
		Integer executionDataCount = insertFavoriteList.size();

		// favoriteAll()を実行(Listで情報取得)
		List<Favorite> favoriteList = service.favoriteAll(insertFavoriteList.get(0).getUserId());

		// データ数が一致しているか確認
		assertEquals(executionDataCount, favoriteList.size());
	}

	@Test
	@DisplayName("お気に入りリストから削除するテスト")
	void testDelete() {
		// テスト用のデータをinsert
		SingleConnectionDataSource singleConnectionDataSource = new SingleConnectionDataSource(
				"jdbc:postgresql://localhost:5432/ec_exam_test", "postgres", "postgres", true);
		NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(singleConnectionDataSource);
		String insertSql = "INSERT INTO favorites(id, user_id, item_id, favorite_date)"
				+ " VALUES('5', '5', '5', '2022-07-26')";
		SqlParameterSource param = new MapSqlParameterSource();
		template.update(insertSql, param);

		// 正常にinsertできたかsqlを発行して確認(※1)
		String sql = "SELECT * FROM favorites WHERE user_id = 5 AND item_id = 5;";
		Favorite favorite = template.queryForObject(sql, param, FAVORITE_ROW_MAPPER);// ここで情報を取得しておく
		assertNotNull(favorite);

		// 削除前のデータ数を取得
		String getNumberOfDataSql = "SELECT COUNT(*) FROM favorites WHERE user_id = 5 AND item_id = 5";
		Integer deleteBeforeCount = template.queryForObject(getNumberOfDataSql, param, Integer.class);

		// deleteの実行
		service.delete(favorite.getUserId(), favorite.getItemId());

		// deleteできたか確認（※1を再度実行しデータがないはずなのでcatchさせる）
		try {
			template.queryForObject(sql, (SqlParameterSource) (null), FAVORITE_ROW_MAPPER);
			fail("失敗です");
		} catch (Exception e) {
			assertTrue(true, "成功です");
		}

		// delete後のデータの数を算出
		Integer deleteAfterCount = template.queryForObject(getNumberOfDataSql, param, Integer.class);

		// deleteの確認テスト
		assertEquals(deleteBeforeCount - 1, deleteAfterCount);

	}

}