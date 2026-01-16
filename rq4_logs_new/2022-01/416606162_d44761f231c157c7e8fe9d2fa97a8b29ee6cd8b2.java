package com.itrex.java.lab.report.repository;

import com.itrex.java.lab.entity.Offer;
import com.itrex.java.lab.report.entity.OfferReportDTO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDate;
import java.util.List;

public interface ReportRepositoryNativeSQL extends JpaRepository<Offer, Integer> {

    @Query(value = "select c.id as contractId, c.description as contractDescription, c.start_price as startPrice, " +
            "u_o_min_price.owner_id as offerOwnerId, u_o_min_price.owner_name as offerOwnerName, offer_id as offerId, " +
            "min_price as minOfferPrice from ( select * from builder.contract " +
            "where start_date >= :firstStartContractDate and start_date <= :endStartContractDate ) as c" +
            "    join (select u.id owner_id, u.name owner_name, o2.id offer_id, o2.contract_id, o2.price min_price " +
            "from builder.user u" +
            "        join (select o.id, o.offer_owner_id, o.contract_id, o.price from builder.offer o" +
            "            join (select o.contract_id, min(o.price) price from (select * from builder.offer " +
            "where offer.contract_id > :startWithContractId) o group by o.contract_id ) as o_min_price " +
            "on o.contract_id = o_min_price.contract_id and o.price = o_min_price.price) o2" +
            "            on u.id = o2.offer_owner_id) as u_o_min_price on u_o_min_price.contract_id = c.id " +
            "order by contract_id limit :size", nativeQuery = true)
    List<OfferReportDTO> getOfferReport(LocalDate firstStartContractDate, LocalDate endStartContractDate,
                                        int startWithContractId, int size);
}