package przemek.spring.petclinic.bootstrap;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import przemek.spring.petclinic.model.Owner;
import przemek.spring.petclinic.model.Vet;
import przemek.spring.petclinic.service.OwnerService;
import przemek.spring.petclinic.service.VetService;
import przemek.spring.petclinic.service.map.OwnerServiceMap;
import przemek.spring.petclinic.service.map.VetServiceMap;

@Component
public class DataLoader implements CommandLineRunner {

    private final OwnerService ownerService;
    private final VetService vetService;

    public DataLoader() {
        this.ownerService = new OwnerServiceMap();
        this.vetService = new VetServiceMap();
    }

    @Override
    public void run(String... args) throws Exception {

        Owner owner = new Owner();
        owner.setId(1L);
        owner.setFirstName("John");
        owner.setLastName("Doe");

        Owner owner2 = new Owner();
        owner.setId(2L);
        owner.setFirstName("Johny");
        owner.setLastName("Bravo");

        ownerService.save(owner);
        ownerService.save(owner2);

        Vet vet1 = new Vet();
        vet1.setId(2L);
        vet1.setFirstName("Sally");
        vet1.setLastName("Adams");

        Vet vet2 = new Vet();
        vet1.setId(2L);
        vet1.setFirstName("Gina");
        vet1.setLastName("Baj");

        vetService.save(vet1);
        vetService.save(vet2);

        System.out.println("Data loaded ");


    }
}