package jm;

import jm.api.dao.ItemDAO;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ItemServiceImpl implements ItemService {

    private final ItemDAO itemDAO;

    public ItemServiceImpl(ItemDAO itemDAO) {
        this.itemDAO = itemDAO;
    }

    @Override
    public void addItemImage(byte[] array) {
        itemDAO.addItemImage(array);
    }

    @Override
    public byte[] getItemImage() {
        return itemDAO.getItemImage();
    }
}