package jm.dao;

import jm.Item;
import jm.api.dao.ItemDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public class ItemDaoImpl extends AbstractDAO<Item> implements ItemDao {
    private static final Logger logger = LoggerFactory.getLogger(ItemDaoImpl.class);

    @Override
    public Optional<Item> getItemByName(String name) {
        try {
            Item item = (Item) entityManager.createNativeQuery("SELECT * FROM items AS i WHERE i.name = :itemname")
                    .setParameter("itemname", name)
                    .getSingleResult();
            logger.debug("ItemDaoImpl. Получен товар: {}", item);
            return Optional.of(item);
        } catch (Exception e) {
            logger.debug("ItemDaoImpl. Не удалось получить товар по имени {}", name, e);
            return Optional.empty();
        }
    }
}