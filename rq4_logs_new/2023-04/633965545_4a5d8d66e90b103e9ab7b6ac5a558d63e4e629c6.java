package com.bdg.service;

import com.bdg.model.Address;
import com.bdg.repository.AddressRepository;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;

import javax.persistence.TypedQuery;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class AddressService implements AddressRepository {

    private Session session;

    @Override
    public Address getBy(int id) {
        checkId(id);

        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();

            com.bdg.persistent.Address addressPer = session.get(com.bdg.persistent.Address.class, id);
            if (addressPer == null) {
                transaction.commit();
                return null;
            }

            Address addressMod = new Address();
            addressMod.setId(addressPer.getId());
            addressMod.setCity(addressPer.getCity());
            addressMod.setCountry(addressPer.getCountry());

            transaction.commit();
            return addressMod;
        } catch (HibernateException e) {
            assert transaction != null;
            transaction.rollback();
            throw new RuntimeException(e);
        }
    }


    @Override
    public Set<Address> getAll() {
        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();

            TypedQuery<com.bdg.persistent.Address> query = session.createQuery("FROM Address", com.bdg.persistent.Address.class);

            List<com.bdg.persistent.Address> addressesPerList = query.getResultList();
            if (addressesPerList.isEmpty()) {
                transaction.commit();
                return null;
            }

            Set<Address> addressesModSet = new LinkedHashSet<>(addressesPerList.size());
            for (com.bdg.persistent.Address tempAddressPer : addressesPerList) {
                Address tempAddressMod = new Address();
                tempAddressMod.setId(tempAddressPer.getId());
                tempAddressMod.setCity(tempAddressPer.getCity());
                tempAddressMod.setCountry(tempAddressPer.getCountry());

                addressesModSet.add(tempAddressMod);
            }

            transaction.commit();
            return addressesModSet;
        } catch (HibernateException e) {
            assert transaction != null;
            transaction.rollback();
            throw new RuntimeException(e);
        }
    }


    @Override
    public Set<Address> get(int offset, int perPage, String sort) {
        if (offset <= 0 || perPage <= 0) {
            throw new IllegalArgumentException("Passed non-positive value as 'offset' or 'perPage': ");
        }
        if (sort == null || sort.isEmpty()) {
            throw new IllegalArgumentException("Passed null or empty value as 'sort': ");
        }
        if (!sort.equals("id") && !sort.equals("country") && !sort.equals("city")) {
            throw new IllegalArgumentException("Parameter 'sort' must be 'id' or 'country' or 'city': ");
        }

        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();

            TypedQuery<List<com.bdg.persistent.Address>> query = session.createQuery("FROM Address order by " + sort);
            query.setFirstResult(offset);
            query.setMaxResults(perPage);

            if (query.getResultList().isEmpty()) {
                transaction.commit();
                return null;
            }

            Set<Address> addressSet = new LinkedHashSet<>(query.getResultList().size());

            for (int i = 0; i < query.getResultList().size(); i++) {
                Address tempAddressMod = new Address();
                com.bdg.persistent.Address tempAddressPer = (com.bdg.persistent.Address) query.getResultList().get(i);

                tempAddressMod.setId(tempAddressPer.getId());
                tempAddressMod.setCountry(tempAddressPer.getCountry());
                tempAddressMod.setCity(tempAddressPer.getCity());

                addressSet.add(tempAddressMod);
            }

            transaction.commit();
            return addressSet.isEmpty() ? null : addressSet;
        } catch (HibernateException e) {
            assert transaction != null;
            transaction.rollback();
            throw new RuntimeException(e);
        }
    }


    @Override
    public Address save(Address item) {
        checkNull(item);

        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();
            com.bdg.persistent.Address addressPer = new com.bdg.persistent.Address();
            addressPer.setCity(item.getCity());
            addressPer.setCountry(item.getCountry());
            session.save(addressPer);
            item.setId(addressPer.getId());

            transaction.commit();
            return item;
        } catch (HibernateException e) {
            assert transaction != null;
            transaction.rollback();
            throw new RuntimeException(e);
        }
    }


    @Override
    public boolean updateBy(int id, Address item) {
        checkId(id);
        checkNull(item);

        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();

            com.bdg.persistent.Address addressPer = session.get(com.bdg.persistent.Address.class, id);

            if (addressPer == null) {
                transaction.commit();
                return false;
            }

            addressPer.setCity(item.getCity());
            addressPer.setCountry(item.getCountry());
            transaction.commit();
            return true;
        } catch (HibernateException e) {
            assert transaction != null;
            transaction.rollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean deleteBy(int id) {
        checkId(id);

        if (existsPassengerBy(id)) {
            System.out.println("First remove address by " + id + " in passenger table: ");
            return false;
        }

        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();

            com.bdg.persistent.Address addressPer = session.get(com.bdg.persistent.Address.class, id);

            if (addressPer == null) {
                transaction.commit();
                return false;
            }

            session.delete(addressPer);
            transaction.commit();
            return true;
        } catch (HibernateException e) {
            assert transaction != null;
            transaction.rollback();
            throw new RuntimeException(e);
        }
    }


    public void setSession(Session session) {
        checkNull(session);
        this.session = session;
    }


    private void checkId(int id) {
        if (id <= 0) {
            throw new IllegalArgumentException("'id' must be a positive number: ");
        }
    }


    private void checkNull(Object item) {
        if (item == null) {
            throw new NullPointerException("Passed null value as 'item': ");
        }
    }


    private boolean existsPassengerBy(int addressId) {
        checkId(addressId);

        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();

            String hql = "select p from Passenger as p where p.address = :addressId";
            TypedQuery<com.bdg.persistent.Passenger> query = session.createQuery(hql);
            query.setParameter("addressId", addressId);

            transaction.commit();
            return !query.getResultList().isEmpty();
        } catch (HibernateException e) {
            assert transaction != null;
            transaction.rollback();
            throw new RuntimeException(e);
        }
    }
}