package com.marketplace;

import com.marketplace.domain.classifiedad.ClassifiedAd;
import com.marketplace.domain.classifiedad.ClassifiedAdId;
import com.marketplace.domain.classifiedad.command.CreateClassifiedAd;
import com.marketplace.domain.classifiedad.command.UpdateClassifiedAdText;
import com.marketplace.domain.classifiedad.command.UpdateClassifiedAdTitle;
import com.marketplace.domain.shared.UserId;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ClassifiedAdFullScenarioTest extends BaseMongoRepositoryTest {
    UUID classifiedAdIdUuid = UUID.fromString("d0b00201-a612-423d-969e-96f182468514");
    UUID ownerIdUuid = UUID.fromString("c205ac9d-d480-4c67-8714-cbbaaf2ae0c4");
    UUID approverIdUuid = UUID.fromString("74c4b183-a7bd-431e-af57-b7377f5c0414");

    @Test
    public void classifiedAdFullScenario() {
        classifiedAdService.handle(CreateClassifiedAd.builder().id(classifiedAdIdUuid)
                .ownerId(ownerIdUuid).build());

        Optional<ClassifiedAd> maybe = classifiedAdService.findById(ClassifiedAdId.from(classifiedAdIdUuid));
        assertThat(maybe).isPresent();
        ClassifiedAd firstClassifiedAd = maybe.get();
        assertThat(firstClassifiedAd.getChanges()).hasSize(1);
        assertThat(firstClassifiedAd.getText()).isNull();
        assertThat(firstClassifiedAd.getTitle()).isNull();
        assertThat(firstClassifiedAd.getApprovedBy()).isNull();
        assertThat(firstClassifiedAd.getId()).isEqualTo(ClassifiedAdId.from(classifiedAdIdUuid));
        assertThat(firstClassifiedAd.getOwnerId()).isEqualTo(UserId.from(ownerIdUuid));

        // update text
        classifiedAdService.handle(UpdateClassifiedAdText.builder().id(classifiedAdIdUuid)
                .text("""
                        2018 Mac mini for sale
                        3.0GHz 6‑core 8th‑generation Intel Core i5 (Turbo Boost up to 4.1GHz)
                        16GB 2666MHz DDR4
                        512GB SSD storage
                        Intel UHD Graphics 630
                        Thanks for looking!""")
                .build());

        Optional<ClassifiedAd> secondMaybe = classifiedAdService.findById(ClassifiedAdId.from(classifiedAdIdUuid));
        assertThat(secondMaybe).isPresent();
        ClassifiedAd secondClassifiedAd = secondMaybe.get();
        assertThat(secondClassifiedAd.getChanges()).hasSize(2);
        assertThat(secondClassifiedAd.getText()).isNotNull();
        assertThat(secondClassifiedAd.getText().value()).isNotBlank().contains("2018 Mac mini");


        // update title
        classifiedAdService.handle(UpdateClassifiedAdTitle.builder().id(classifiedAdIdUuid)
                .title("2018 Mac Mini 3.0GHz 6‑core 16GB 512GB SSD - $1,000 (Golden Valley)")
                .build());

        Optional<ClassifiedAd> thirdMaybe = classifiedAdService.findById(ClassifiedAdId.from(classifiedAdIdUuid));
        assertThat(thirdMaybe).isPresent();
        ClassifiedAd thirdClassifiedAd = thirdMaybe.get();
        assertThat(thirdClassifiedAd.getChanges()).hasSize(3);
        assertThat(thirdClassifiedAd.getText()).isNotNull();
        assertThat(thirdClassifiedAd.getTitle()).isNotNull();
        assertThat(thirdClassifiedAd.getText().value()).isNotBlank().contains("2018 Mac mini");
        assertThat(thirdClassifiedAd.getTitle().value()).isNotBlank().contains("2018 Mac Mini");

        // update title
        classifiedAdService.handle(UpdateClassifiedAdTitle.builder().id(classifiedAdIdUuid)
                .title("2018 Mac Mini 3.0GHz 6‑core 16GB 512GB SSD - $1,000 (Golden Valley)")
                .build());

        Optional<ClassifiedAd> fourthMaybe = classifiedAdService.findById(ClassifiedAdId.from(classifiedAdIdUuid));
        assertThat(fourthMaybe).isPresent();
        ClassifiedAd fourthClassifiedAd = fourthMaybe.get();
        assertThat(fourthClassifiedAd.getChanges()).hasSize(3);
        assertThat(fourthClassifiedAd.getText()).isNotNull();
        assertThat(fourthClassifiedAd.getTitle()).isNotNull();
        assertThat(fourthClassifiedAd.getText().value()).isNotBlank().contains("2018 Mac mini");
        assertThat(fourthClassifiedAd.getTitle().value()).isNotBlank().contains("2018 Mac Mini");

    }
}