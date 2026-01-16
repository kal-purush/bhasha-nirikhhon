package org.spiceboys.Travel.Diary.seeder;

import org.spiceboys.Travel.Diary.model.Country;
import org.spiceboys.Travel.Diary.repository.CountryRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class DataSeeder implements CommandLineRunner {

    private final CountryRepository countryRepository;

    public DataSeeder(CountryRepository countryRepository) {
        this.countryRepository = countryRepository;
    }

    @Override
    public void run(String... args) throws Exception {
        if (countryRepository.count() == 0) {
            Country france = new Country(
                    "France",
                    "A beautiful country known for its cuisine and the Eiffel Tower.",
                    "https://upload.wikimedia.org/wikipedia/en/c/c3/Flag_of_France.svg"
            );

            Country japan = new Country(
                    "Japan",
                    "A country rich in tradition and technology.",
                    "https://upload.wikimedia.org/wikipedia/en/9/9e/Flag_of_Japan.svg"
            );

            Country brazil = new Country(
                    "Brazil",
                    "A country famous for its carnivals and the Amazon rainforest.",
                    "https://upload.wikimedia.org/wikipedia/en/0/05/Flag_of_Brazil.svg"
            );

            Country australia = new Country(
                    "Australia",
                    "A vast country known for its unique wildlife and the Great Barrier Reef.",
                    "https://upload.wikimedia.org/wikipedia/commons/b/b9/Flag_of_Australia.svg"
            );

            Country canada = new Country(
                    "Canada",
                    "A beautiful country known for its vast wilderness and the Rocky Mountains.",
                    "https://upload.wikimedia.org/wikipedia/commons/c/cf/Flag_of_Canada.svg"
            );

            Country india = new Country(
                    "India",
                    "A country rich in culture, history, and diverse landscapes.",
                    "https://upload.wikimedia.org/wikipedia/en/4/41/Flag_of_India.svg"
            );

            Country italy = new Country(
                    "Italy",
                    "A country known for its art, architecture, and delicious food.",
                    "https://upload.wikimedia.org/wikipedia/commons/0/03/Flag_of_Italy.svg"
            );

            Country mexico = new Country(
                    "Mexico",
                    "A country with a rich history, vibrant culture, and delicious cuisine.",
                    "https://upload.wikimedia.org/wikipedia/commons/f/fc/Flag_of_Mexico.svg"
            );

            Country germany = new Country(
                    "Germany",
                    "A country known for its engineering, history, and cultural contributions.",
                    "https://upload.wikimedia.org/wikipedia/commons/b/ba/Flag_of_Germany.svg"
            );

            Country southKorea = new Country(
                    "South Korea",
                    "A country that blends modern technology with traditional culture.",
                    "https://upload.wikimedia.org/wikipedia/commons/0/0f/Flag_of_South_Korea.svg"
            );

            countryRepository.save(france);
            countryRepository.save(japan);
            countryRepository.save(brazil);
            countryRepository.save(australia);
            countryRepository.save(canada);
            countryRepository.save(india);
            countryRepository.save(italy);
            countryRepository.save(mexico);
            countryRepository.save(germany);
            countryRepository.save(southKorea);

            System.out.println("✅ Countries table seeded!");
        } else {
            System.out.println("ℹ️ Countries table already has data, skipping seed.");
        }
    }
}