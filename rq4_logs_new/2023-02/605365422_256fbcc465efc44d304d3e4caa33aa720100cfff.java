package com.example.finalproject;

import com.example.finalproject.model.Bill;
import com.example.finalproject.model.Feature;
import com.example.finalproject.repository.BillRepository;
import com.example.finalproject.service.BillServices;
import org.junit.jupiter.api.BeforeEach;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.*;
@ExtendWith(MockitoExtension.class)
public class BillServiceTest {
    @InjectMocks
    BillServices billServices;

    @Mock
    BillRepository billRepository;

    Bill bill1;

    @BeforeEach
    void setUp(){
        bill1 = new Bill(null, 23.2, "cash", 10.0);
    }
    @Test
    public void getFeature() {
        billRepository.save(bill1);
        when(billRepository.findBillById(bill1.getId())).thenReturn(bill1);
        Bill bill = billRepository.findBillById(bill1.getId());
        //Note//read above note to understand
        Assertions.assertThat(bill1).isEqualTo(bill);

        verify(billRepository, times(1)).findBillById(bill1.getId());
    }


}