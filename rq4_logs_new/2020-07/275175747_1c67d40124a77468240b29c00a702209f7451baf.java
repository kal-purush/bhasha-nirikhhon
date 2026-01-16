package jm.stockx;

import jm.stockx.api.dao.BidDAO;
import jm.stockx.entity.Bid;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Transactional
public class BidServiceImpl implements BidService {

    private final BidDAO bidDAO;

    public BidServiceImpl(BidDAO bidDAO) {
        this.bidDAO = bidDAO;
    }

    @Override
    public List<Bid> getAll() {
        return bidDAO.getAll();
    }

    @Override
    public Bid get(Long id) {
        return bidDAO.getById(id);
    }

    @Override
    public void create(Bid bid) {
        bidDAO.add(bid);
    }

    @Override
    public void update(Bid bid) {
        bidDAO.update(bid);
    }

    @Override
    public void delete(Long id) {
        bidDAO.deleteById(id);
    }
}