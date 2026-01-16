package com.ac.common.fabric;

import com.ac.common.constant.SmartContractConstant;
import com.ac.common.fabric.model.ChainCodeResultModel;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.io.File;

/**
 * Created by zhenchao.bi on 6/27/2017.
 */
@Service
public class SmartContractWapper {

    @Autowired
    private ChannelWapper channel;

    private ResourceLoader loader = new DefaultResourceLoader();


    public boolean installHospitalSC() {

        try {
            //smartContract\hospital
            File scFile = loader.getResource("classpath:/smartContract/hospital").getFile();

            //File chaincodeSourceLocation, String chaincodeName,
            //String chaincodeVersion, String path, Collection< Peer > peers
            ChainCodeResultModel chainCodeResultModel = channel.installChaincode(scFile, SmartContractConstant.Hospital.CHAINCODE_NAME, SmartContractConstant.Hospital.CHAINCODE_VERSION,
                    SmartContractConstant.Hospital.CHAINCODE_PATH, channel.getAllPeers());

            if (CollectionUtils.isEmpty(chainCodeResultModel.getFailed())) {
                return true;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return false;
    }

}