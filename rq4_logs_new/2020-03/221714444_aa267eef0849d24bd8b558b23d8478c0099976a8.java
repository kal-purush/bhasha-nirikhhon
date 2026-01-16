package com.zakrzewski.intentionbook.configurations;

import com.zakrzewski.intentionbook.abstractClass.ChurchWorker;
import com.zakrzewski.intentionbook.entities.BookOfIntentionModel;
import com.zakrzewski.intentionbook.entities.PriestModel;
import com.zakrzewski.intentionbook.entities.SacristanModel;
import com.zakrzewski.intentionbook.enums.AccessEnum;
import com.zakrzewski.intentionbook.repositories.BookOfIntentionRepository;
import com.zakrzewski.intentionbook.repositories.ChurchWorkerRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.time.LocalDate;
import java.time.LocalTime;

@Configuration
public class TemplateConfiguration {

    private final BookOfIntentionRepository bookOfIntentionRepository;
    private final ChurchWorkerRepository churchWorkerRepository;

    @Autowired
    public TemplateConfiguration(BookOfIntentionRepository bookOfIntentionRepository, ChurchWorkerRepository churchWorkerRepository) {
        this.bookOfIntentionRepository = bookOfIntentionRepository;
        this.churchWorkerRepository = churchWorkerRepository;
    }

    @Bean
    public PasswordEncoder passwordEncoder(){
        return new BCryptPasswordEncoder();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void createFinalTemplate() {
        ChurchWorker priest1 = new PriestModel("priest1", passwordEncoder().encode("qwerty") , "Damian", "Kosz", AccessEnum.USER_KAPLAN.getRoleDescription());
        ChurchWorker priest2 = new PriestModel("priest2", passwordEncoder().encode("qwerty"), "Wiktor", "Dalas", AccessEnum.USER_KAPLAN.getRoleDescription());
        ChurchWorker priest3 = new PriestModel("superpriest", passwordEncoder().encode("qwerty"),"Grzegorz", "Sikora", AccessEnum.SUPER_USER.getRoleDescription());
        ChurchWorker sacristian1 = new SacristanModel("zakr1", passwordEncoder().encode("qwerty"), "Bogusław", "Pietras", AccessEnum.USER_ZAKRYS.getRoleDescription());
        ChurchWorker sacristian2 = new SacristanModel("zakr2", passwordEncoder().encode("qwerty"), "Ryszard", "Gosztyła", AccessEnum.USER_ZAKRYS.getRoleDescription());
        churchWorkerRepository.save(priest1);
        churchWorkerRepository.save(priest2);
        churchWorkerRepository.save(priest3);
        churchWorkerRepository.save(sacristian1);
        churchWorkerRepository.save(sacristian2);
        BookOfIntentionModel intentionModel1 = new BookOfIntentionModel(LocalDate.of(2020, 1, 20), LocalTime.of(8, 0), "W dniu 19 urodzin Marysi.", priest1.getFirstName() + " " + priest1.getLastName(), "brak", "100zł", sacristian1);
        BookOfIntentionModel intentionModel2 = new BookOfIntentionModel(LocalDate.of(2020, 2, 12), LocalTime.of(18, 0), "Za + Izoldę w 13 rocznicę śmierci.", priest2.getFirstName() + " " + priest2.getLastName(), "brak", "80zł", priest2);
        BookOfIntentionModel intentionModel3 = new BookOfIntentionModel(LocalDate.of(2020, 2, 22), LocalTime.of(7, 0), "Za darczyńców.", priest1.getFirstName() + " " + priest1.getLastName(), "brak", "50zł", priest1);
        BookOfIntentionModel intentionModel4 = new BookOfIntentionModel(LocalDate.of(2020, 3, 1), LocalTime.of(6, 30), "Za dusze w czyścu cierpiące.", priest2.getFirstName() + " " + priest2.getLastName(), "brak", "40zł", sacristian1);
        bookOfIntentionRepository.save(intentionModel1);
        bookOfIntentionRepository.save(intentionModel2);
        bookOfIntentionRepository.save(intentionModel3);
        bookOfIntentionRepository.save(intentionModel4);
    }
}