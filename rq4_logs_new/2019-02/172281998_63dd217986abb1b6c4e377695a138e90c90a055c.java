package com.integration.demos.batch.readers;

import com.integration.demos.model.Benefitiary;
import org.springframework.batch.item.*;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class BenifitiaryCustomItemReader<Benefitiary>  implements ItemReader<Benefitiary> , ItemStream{

    List<Benefitiary> benefitiaries;
    int currentIndex = 0;
    private static final String CURRENT_INDEX = "bene.items.current.index";

    public BenifitiaryCustomItemReader(List<Benefitiary> benefitiaries){
        this.benefitiaries = benefitiaries;
    }

    @Override
    public Benefitiary read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {

        if(CollectionUtils.isEmpty(this.benefitiaries) && currentIndex < benefitiaries.size()){
            benefitiaries.get(currentIndex++);
        }

        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if(executionContext.containsKey(CURRENT_INDEX)){
            currentIndex = new Long(executionContext.getLong(CURRENT_INDEX)).intValue();
        }
        else{
            currentIndex = 0;
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putLong(CURRENT_INDEX, new Long(currentIndex).longValue());
    }

    @Override
    public void close() throws ItemStreamException { }
}