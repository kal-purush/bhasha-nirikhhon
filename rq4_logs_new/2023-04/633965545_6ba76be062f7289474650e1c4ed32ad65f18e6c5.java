package com.bdg.service;

import com.bdg.hibernate.HibernateUtil;
import com.bdg.model.AddressMod;
import com.bdg.model.PassengerMod;
import com.bdg.persistent.AddressPer;
import com.bdg.persistent.PassengerPer;
import com.bdg.repository.PassengerRepository;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.util.List;
import java.util.Set;

public class PassengerService implements PassengerRepository {

    private final Session session;

    private static final AddressService ADDRESS_SERVICE = new AddressService();

    public PassengerService() {
        session = HibernateUtil.getSession();
    }

    @Override
    public List<PassengerMod> getAllOf(int tripId) {
        return null;
    }

    @Override
    public boolean registerTrip(int id, int tripId) {
        return false;
    }

    @Override
    public boolean cancelTrip(int id, int tripId) {
        return false;
    }

    @Override
    public PassengerMod getBy(int id) {
        checkId(id);

        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();

            PassengerPer passengerPer = session.get(PassengerPer.class, id);
            if (passengerPer == null) {
                transaction.rollback();
                return null;
            }

            PassengerMod passengerMod = new PassengerMod();
            passengerMod.setId(passengerPer.getId());
            passengerMod.setName(passengerPer.getName());
            passengerMod.setPhone(passengerPer.getPhone());

            AddressMod addressMod = new AddressMod();
            if (passengerPer.getAddress() == null) {
                passengerMod.setAddress(null);

                transaction.commit();
                return passengerMod;
            }

            addressMod.setId(passengerPer.getAddress().getId());
            addressMod.setCountry(passengerPer.getAddress().getCountry());
            addressMod.setCity(passengerPer.getAddress().getCity());

            passengerMod.setAddress(addressMod);

            transaction.commit();
            return passengerMod;
        } catch (HibernateException e) {
            assert transaction != null;
            transaction.rollback();
            throw new RuntimeException(e);
        }
    }


    @Override
    public Set<PassengerMod> getAll() {
        return null;
    }

    @Override
    public Set<PassengerMod> get(int offset, int perPage, String sort) {
        return null;
    }

    @Override
    public PassengerMod save(PassengerMod item) {
        checkNull(item);

        int idOfAddress = ADDRESS_SERVICE.getId(item.getAddress());

        Transaction transaction = null;
        try {
            transaction = session.beginTransaction();

            PassengerPer passengerPer = new PassengerPer();
            passengerPer.setName(item.getName());
            passengerPer.setPhone(item.getPhone());

            if (idOfAddress > 0) {
                passengerPer.setAddress(session.get(AddressPer.class, idOfAddress));
            } else {
                AddressPer notExistingAddressPer = new AddressPer();
                notExistingAddressPer.setCountry(item.getAddress().getCountry());
                notExistingAddressPer.setCity(item.getAddress().getCity());

                session.save(notExistingAddressPer);
                passengerPer.setAddress(notExistingAddressPer);
            }

            session.save(passengerPer);
            item.setId(passengerPer.getId());

            transaction.commit();
            return item;
        } catch (HibernateException e) {
            assert transaction != null;
            transaction.rollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean updateBy(int id, PassengerMod item) {
        checkId(id);
        checkNull(item);

        return false;
    }

    @Override
    public boolean deleteBy(int id) {
        checkId(id);
        return false;
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
}