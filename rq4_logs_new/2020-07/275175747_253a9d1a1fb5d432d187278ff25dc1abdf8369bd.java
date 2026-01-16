package jm.dao;

import jm.Brand;
import jm.api.dao.BrandDAO;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class BrandDaoImpl extends AbstractDAO<Brand> implements BrandDAO {

    @Override
    public Optional<Brand> getBrandByName(String name) {
        try {
            Brand brand = entityManager.createQuery("FROM Brand WHERE name = :brandName", Brand.class)
                    .setParameter("brandName", name)
                    .getSingleResult();
            return Optional.of(brand);
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}