package com.fisco.app.client;

import com.fisco.app.common.CommonClient;
import com.fisco.app.contract.PetContract;
import com.fisco.app.entity.Pet;
import com.fisco.app.utils.SpringUtils;
import org.fisco.bcos.sdk.v3.BcosSDK;
import org.fisco.bcos.sdk.v3.codec.datatypes.generated.tuples.generated.Tuple8;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class PetClient extends CommonClient implements ApplicationRunner {

    public void add_pet(String pet_name, String sell_username, String picture_path,
                       String description, double price,
                       String pet_class, boolean has_sold_out) {
        PetContract petContract = (PetContract) getContractMap().get("PetContract");
        petContract.add_pet(pet_name, sell_username, picture_path, description, price, pet_class, has_sold_out);
        logger.info("调用PetClient的add_pet方法");
        logger.info("结果：{}", "void");
    }

    public Pet query_pet_by_id(int pet_id) {
        PetContract petContract = (PetContract) getContractMap().get("PetContract");
        Tuple8 <Integer, String, String, String , String, Double, String, Boolean> get_value = petContract.query_pet_by_id(pet_id);
        logger.info("调用PetClient的query_pet_by_id方法");
        logger.info("结果：{}", get_value);
        if(get_value == null)
            return null;
        else
            return package_tuple8_to_pet(get_value);
    }

    public List<Pet> query_pets_by_username(String username) {
        PetContract petContract = (PetContract) getContractMap().get("PetContract");
        List< Tuple8<Integer, String, String, String , String, Double, String, Boolean> > list = petContract.query_pets_by_username(username);
        logger.info("调用PetClient的query_pet_by_id方法");
        logger.info("结果：{}", list);
        if(list.size() == 0)
            return new ArrayList<Pet>();
        else {
            List<Pet> pets = new ArrayList<>(list.size());
            for (Tuple8<Integer, String, String, String , String, Double, String, Boolean> tuple:list){
                pets.add(package_tuple8_to_pet(tuple));
            }
            return pets;
        }
    }

    public List<Pet> query_all_pets() {
        PetContract petContract = (PetContract) getContractMap().get("PetContract");
        List< Tuple8<Integer, String, String, String , String, Double, String, Boolean> > list = petContract.query_all_pets();
        logger.info("调用PetClient的query_pet_by_id方法");
        logger.info("结果：{}", list);
        if(list.size() == 0)
            return new ArrayList<Pet>();
        else {
            List<Pet> pets = new ArrayList<>(list.size());
            for (Tuple8<Integer, String, String, String , String, Double, String, Boolean> tuple:list){
                pets.add(package_tuple8_to_pet(tuple));
            }
            return pets;
        }
    }

    public void set_pet_has_sold_out(int pet_id) {
        PetContract petContract = (PetContract) getContractMap().get("PetContract");
        petContract.set_pet_has_sold_out(pet_id);
        logger.info("调用PetClient的set_pet_has_sold_out方法");
        logger.info("结果：{}", "void");
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        BcosSDK sdk = SpringUtils.getBean("bcosSDK");
        deploy("PetContract", PetContract.class, sdk);
    }

    /**
     * 将Tuple8打包成Pet对象
     */
    private static Pet package_tuple8_to_pet(Tuple8 <Integer, String, String, String , String, Double, String, Boolean> get_value) {
        return new Pet(get_value.getValue1(), get_value.getValue2(), get_value.getValue3(),
                get_value.getValue4(), get_value.getValue5(), get_value.getValue6(),
                get_value.getValue7(), get_value.getValue8());
    }
}