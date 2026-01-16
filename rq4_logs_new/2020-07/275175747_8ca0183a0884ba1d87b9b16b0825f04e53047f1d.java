package jm.stockx.api.dao;

import jm.stockx.dto.BuyingInfoDto;
import jm.stockx.entity.BuyingInfo;
import org.springframework.stereotype.Repository;

@Repository
public class BuyingInfoDaoImpl extends AbstractDAO<BuyingInfo, Long> implements BuyingInfoDAO {

    @Override
    public BuyingInfoDto getBuyingInfoDtoById(Long id) {
        String query =
                "SELECT NEW jm.stockx.dto.BuyingInfoDto(b.id, b.buyingTimeStamp, b.buyingPrice)" +
                        "FROM BuyingInfo AS b " +
                        "WHERE id =: id";

        return entityManager.createQuery(query, BuyingInfoDto.class)
                .setParameter("id", id).getSingleResult();
    }
}