package com.upgrad.FoodOrderingApp.service.business;

import com.upgrad.FoodOrderingApp.service.common.ApplicationUtil;
import org.springframework.stereotype.Service;
import com.upgrad.FoodOrderingApp.service.dao.*;
import com.upgrad.FoodOrderingApp.service.entity.*;
import com.upgrad.FoodOrderingApp.service.exception.AddressNotFoundException;
import com.upgrad.FoodOrderingApp.service.exception.AuthorizationFailedException;
import com.upgrad.FoodOrderingApp.service.exception.SaveAddressException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.util.LinkedList;
import java.util.List;

/**
 * @author zeelani
 * Service class handling the business logic for the address controller
 */

@Service
public class AddressService {

    @Autowired
    OrderDao orderDao;

    @Autowired
    StateDao stateDao;

    @Autowired
    AddressDao addressDao;

    @Autowired
    CustomerAuthDao customerAuthDao;

    @Autowired
    ApplicationUtil applicationUtil;

    @Autowired
    CustomerAddressDao customerAddressDao;



    /**
     * Method to persist the address
     * @param addressEntity
     * @param stateEntity
     * @return
     * @throws SaveAddressException
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public AddressEntity saveAddress(AddressEntity addressEntity,StateEntity stateEntity)throws SaveAddressException{
        if (addressEntity.getCity() == null || addressEntity.getFlatBuilNo() == null || addressEntity.getPincode() == null || addressEntity.getLocality() == null){
            throw new SaveAddressException("SAR-001","No field can be empty");
        }
        if(!applicationUtil.valdiatePinCode(addressEntity.getPincode())){
            throw new SaveAddressException("SAR-002","Invalid pincode");
        }
        addressEntity.setState(stateEntity);
        AddressEntity updatedAddressEntity = addressDao.saveAddress(addressEntity);
        return updatedAddressEntity;
    }

    /**
     * Method to get all the address of the customer
     * @param customerEntity
     * @return
     */
    public List<AddressEntity> getAllAddress(CustomerEntity customerEntity) {
        List<AddressEntity> addressEntities = new LinkedList<>();
        List<CustomerAddressEntity> customerAddressEntities  = customerAddressDao.fetchAllCustomerAddress(customerEntity);
        if(customerAddressEntities != null) {
            customerAddressEntities.forEach(customerAddressEntity -> {
                addressEntities.add(customerAddressEntity.getAddress());
            });
        }
        return addressEntities;
    }

    /**
     * method to get the state by uuid
     * @param uuid
     * @return
     * @throws AddressNotFoundException
     */
    public StateEntity getStateByUUID(String uuid) throws AddressNotFoundException{
        StateEntity stateEntity = stateDao.fetchByUuid(uuid);
        if(stateEntity == null) {
            throw new AddressNotFoundException("ANF-002", "No state by this id");
        }
        return  stateEntity;
    }

    /**
     * Method to save the customer address
     * @param customerEntity
     * @param addressEntity
     * @return
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public CustomerAddressEntity saveCustomerAddressEntity(CustomerEntity customerEntity,AddressEntity addressEntity){
        CustomerAddressEntity customerAddressEntity = new CustomerAddressEntity();
        customerAddressEntity.setCustomer(customerEntity);
        customerAddressEntity.setAddress(addressEntity);
        CustomerAddressEntity createdCustomerAddressEntity = customerAddressDao.saveCustomerAddress(customerAddressEntity);
        return createdCustomerAddressEntity;
    }

    /**
     * Method to get the address by id
     * @param addressUuid
     * @param customerEntity
     * @return
     * @throws AuthorizationFailedException
     * @throws AddressNotFoundException
     */
    public AddressEntity getAddressByUUID(String addressUuid,CustomerEntity customerEntity)throws AuthorizationFailedException,AddressNotFoundException{
        if(addressUuid == null){
            throw new AddressNotFoundException("ANF-005","Address id can not be empty");
        }
        AddressEntity addressEntity = addressDao.fetchByUuid(addressUuid);
        if (addressEntity == null){
            throw new AddressNotFoundException("ANF-003","No address by this id");
        }
        CustomerAddressEntity customerAddressEntity = customerAddressDao.fetchCustomerByAddress(addressEntity);

        // the address must belong to the requested customer
        if(customerAddressEntity.getCustomer().getUuid() == customerEntity.getUuid()){
            return addressEntity;
        }else{
            throw new AuthorizationFailedException("ATHR-004","You are not authorized to view/update/delete any one else's address");
        }
    }

    /**
     * Method to delete the address
     * @param addressEntity
     * @return
     */
    @Transactional(propagation = Propagation.REQUIRED)
    public AddressEntity deleteAddress(AddressEntity addressEntity) {
        List<OrdersEntity> ordersEntities = orderDao.fetchByAddress(addressEntity);
        if(ordersEntities == null||ordersEntities.isEmpty()) {
            AddressEntity deletedAddressEntity = addressDao.delete(addressEntity);
            return deletedAddressEntity;
        }else{
            addressEntity.setActive(0);
            AddressEntity updatedAddressActiveStatus =  addressDao.updateActiveStatus(addressEntity);
            return updatedAddressActiveStatus;
        }
    }

    /**
     * Method to get all states
     * @return
     */
    public List<StateEntity> getAllStates(){
        List<StateEntity> stateEntities = stateDao.fetchAll();
        return stateEntities;
    }
}
