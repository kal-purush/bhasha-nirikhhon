package jm.stockx;

import jm.stockx.api.dao.BuyingDAO;
import jm.stockx.entity.BuyingInfo;
import jm.stockx.enums.Status;
import org.springframework.transaction.annotation.Transactional;

public class BuyServiceImpl implements BuyService {
    private final BuyingDAO buyingDAO;

    public BuyServiceImpl(BuyingDAO buyingDAO) {
        this.buyingDAO = buyingDAO;
    }

    @Transactional
    @Override
    public void updateBuyingStatus(Long id, Status status) {
        BuyingInfo buyingInfo = buyingDAO.getById(id);
        buyingInfo.setStatus(status);
        buyingDAO.update(buyingInfo);
    }
}