package jm.stockx;

import jm.stockx.api.dao.SellingDAO;
import jm.stockx.dto.SellerTopInfo;
import jm.stockx.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class SellingInfoServiceImpl implements SellingInfoService{

    private final SellingDAO sellingDAO;

    @Autowired
    public SellingInfoServiceImpl(SellingDAO sellingDAO){
        this.sellingDAO = sellingDAO;
    }

    @Override
    public List<SellerTopInfo> getTop20SellingUsers() {
        List<User> sellers = sellingDAO.getTop20SellingUsers();
        List<SellerTopInfo> usersNames = new ArrayList<>();
        for (User s : sellers){
            usersNames.add(new SellerTopInfo(s.getUsername()));
        }
        return usersNames;
    }
}