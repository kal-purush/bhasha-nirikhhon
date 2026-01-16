package com.fpr.financialProduct.repository;

import com.fpr.financialProduct.DTO.FinancialProduct;
import com.fpr.financialProduct.DTO.FinancialProductOption;
import com.fpr.financialProduct.entity.FinancialProductEntity;
import com.fpr.financialProduct.entity.FinancialProductOptionEntity;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.transaction.Transactional;
import lombok.Data;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Data
@Repository
@Transactional
public class JpaFinancialProductOptionRepository {
    private final EntityManager em;

    public FinancialProductOptionEntity save(FinancialProduct financialProduct, FinancialProductOption financialProductOption) {
        //financialProductEntity와 financialProductOptionEntity의 관계 설정
        FinancialProductEntity financialProductEntity = new FinancialProductEntity(financialProduct);
        FinancialProductOptionEntity financialProductOptionEntity = new FinancialProductOptionEntity(financialProductEntity, financialProductOption);

        //FinancialOptionEntity가 이미 존재한다면 저장하지 않음
        if (findByCode_SaveTerm(financialProductEntity.getFinancialProductCode(), financialProductOptionEntity.getSaveTerm()).isEmpty()) {
            em.persist(financialProductOptionEntity);
        }

        return financialProductOptionEntity;
    }

    //중복확인 메서드 code+saveTerm 은 키이므로 유일해야함
    public Optional<FinancialProductOptionEntity> findByCode_SaveTerm(String financialProductCode, String saveTerm) {
        String jpql = "select f from FinancialProductOption f where f.financialProduct.financialProductCode = :financialProductCode and f.saveTerm = :saveTerm";
        TypedQuery<FinancialProductOptionEntity> query = em.createQuery(jpql, FinancialProductOptionEntity.class);
        query.setParameter("financialProductCode", financialProductCode);
        query.setParameter("saveTerm", saveTerm);
        FinancialProductOptionEntity financialProductOptionEntity = query.getSingleResult();

        return Optional.ofNullable(financialProductOptionEntity);
    }
}