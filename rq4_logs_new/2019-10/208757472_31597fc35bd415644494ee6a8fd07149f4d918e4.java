package mate.academy.internetshop.dao.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import mate.academy.internetshop.dao.BucketDao;
import mate.academy.internetshop.lib.Dao;
import mate.academy.internetshop.model.Bucket;
import mate.academy.internetshop.model.Item;
import org.apache.log4j.Logger;

@Dao
public class BucketDaoJdbcImpl extends AbstractDao<Bucket> implements BucketDao {
    private static Logger logger = Logger.getLogger(BucketDaoJdbcImpl.class);
    private static String DB_NAME = "internetshop";

    public BucketDaoJdbcImpl(Connection connection) {
        super(connection);
    }

    @Override
    public Bucket create(Bucket bucket) {
        String query = "INSERT INTO " + DB_NAME + ".buckets (user_id) VALUES (?);";
        try (PreparedStatement statement
                     = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            Long userId = bucket.getUserId();
            statement.setLong(1, userId);
            statement.executeUpdate();
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                bucket.setId(resultSet.getLong(1));
            }
        } catch (SQLException e) {
            logger.error("Can't create bucket", e);
        }
        return bucket;
    }

    @Override
    public Optional<Bucket> get(Long id) {
        String query = "SELECT * FROM " + DB_NAME
                + ".buckets WHERE bucket_id = ?;";
        Bucket bucket = null;
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, id);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                Long bucketId = resultSet.getLong("bucket_id");
                Long userId = resultSet.getLong("user_id");
                bucket = new Bucket();
                bucket.setId(bucketId);
                bucket.setUserId(userId);
                List<Item> items = getItems(bucketId);
                bucket.setItems(items);
            }
        } catch (SQLException e) {
            logger.error("Can't get bucket by id = " + id, e);
        }
        return Optional.of(bucket);
    }

    private List<Item> getItems(Long bucketId) {
        List<Item> items = new ArrayList<>();
        String query = "SELECT * " + "FROM "
                + DB_NAME + ".items INNER JOIN "
                + DB_NAME + ".buckets_items "
                + "ON " + DB_NAME + ".buckets_items.item_id=" + DB_NAME
                + ".items.item_id " + "WHERE buckets_items.bucket_id=?;";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, bucketId);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Long itemId = resultSet.getLong("item_id");
                String name = resultSet.getString("name");
                Double price = resultSet.getDouble("price");
                Item item = new Item();
                item.setName(name);
                item.setPrice(price);
                item.setId(itemId);
                items.add(item);
            }
        } catch (SQLException e) {
            logger.error("Getting items from bucket failed", e);
        }
        return items;
    }

    @Override
    public Bucket update(Bucket bucket) {
        String query = "UPDATE " + DB_NAME
                + ".buckets SET user_id = ? WHERE bucket_id = ?;";
        try (PreparedStatement statementBuckets = connection.prepareStatement(query,
                Statement.RETURN_GENERATED_KEYS)) {
            statementBuckets.setLong(1, bucket.getUserId());
            statementBuckets.setLong(2, bucket.getId());
            statementBuckets.executeUpdate();
            ResultSet keys = statementBuckets.getGeneratedKeys();
            if (keys.next()) {
                bucket.setId(keys.getLong("bucket_id"));
            }
        } catch (SQLException e) {
            logger.error("Can't update bucket", e);
        }
        return bucket;
    }

    @Override
    public Bucket delete(Long id) {
        return null;
    }

    @Override
    public Bucket addItem(Long bucketId, Long itemId) {
        String query = "INSERT INTO " + DB_NAME
                + ".buckets_items (bucket_id, item_id) VALUES (?, ?);";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, bucketId);
            statement.setLong(2, itemId);
            statement.executeUpdate();
        } catch (SQLException e) {
            logger.error("Can't add item to bucket", e);
        }
        return get(bucketId).orElseThrow();
    }

    @Override
    public void removeItem(Long bucketId, Long itemId) {
        String query = "DELETE FROM " + DB_NAME
                + ".buckets_items WHERE bucket_id = ? AND item_id = ?;";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, bucketId);
            statement.setLong(2, itemId);
            statement.executeUpdate();
        } catch (SQLException e) {
            logger.error("Can't delete item from bucket with id = "
                    + bucketId, e);
        }
    }

    @Override
    public void clear(Long bucketId) {
        String query = "DELETE FROM " + DB_NAME
                + ".buckets_items WHERE bucket_id = ?;";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, bucketId);
            statement.executeUpdate();
        } catch (SQLException e) {
            logger.error("Can't clear bucket with id = " + bucketId, e);
        }
    }

    @Override
    public Optional<Bucket> getBucketByUserId(Long userId) {
        String query = "SELECT " + DB_NAME + ".buckets.bucket_id FROM "
                + DB_NAME + ".buckets WHERE user_id = ?;";
        Long bucketId = null;
        Bucket bucket = null;
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, userId);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                bucketId = resultSet.getLong("bucket_id");
            }
        } catch (SQLException e) {
            logger.error("Get bucket by id error", e);
        }
        return get(bucketId);
    }
}