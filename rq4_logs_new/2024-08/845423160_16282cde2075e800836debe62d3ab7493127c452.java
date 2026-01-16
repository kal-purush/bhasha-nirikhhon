package fiveguys.innout.repository;

import fiveguys.innout.entity.Category;
import fiveguys.innout.entity.Transaction;
import fiveguys.innout.entity.User;
import fiveguys.innout.repository.CategoryRepository;
import fiveguys.innout.repository.TransactionRepository;
import fiveguys.innout.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Date;
import java.util.List;


import java.util.Arrays;
import java.util.Date;
import java.util.List;

@RequiredArgsConstructor
@Component
public class InnoutRunner implements ApplicationRunner {

    @Autowired
    private CategoryRepository categoryRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private BCryptPasswordEncoder passwordEncoder;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (userRepository.count() == 0 && categoryRepository.count() == 0 && transactionRepository.count() == 0) {
            System.out.println("InnoutRunner is executing...");

            // 예시 데이터 추가 로직
            List<Category> categories = Arrays.asList(
                    createCategory(1L, "식비"),
                    createCategory(2L, "교통"),
                    createCategory(3L, "패션"),
                    createCategory(4L, "문화생활"),
                    createCategory(5L, "교육"),
                    createCategory(6L, "기타")
            );

            categoryRepository.saveAll(categories);
            System.out.println("Categories saved");

            // 사용자 데이터 추가
            User user1 = new User();
            user1.setName("Alice");
            user1.setEmail("alice@example.com");
            user1.setPassword(passwordEncoder.encode("password123"));
            user1.setBirthDate(new Date(90, 1, 1)); // Date 생성자 사용 주의: 연도는 1900년 기준
            user1.setGender("여성");

            User user2 = new User();
            user2.setName("Bob");
            user2.setEmail("bob@example.com");
            user2.setPassword(passwordEncoder.encode("password123"));
            user2.setBirthDate(new Date(85, 2, 15)); // Date 생성자 사용 주의: 연도는 1900년 기준
            user2.setGender("남성");

            userRepository.saveAll(List.of(user1, user2));
            System.out.println("Users saved");

            // 트랜잭션 데이터 추가
            Transaction transaction1 = new Transaction();
            transaction1.setDate(new Date());
            transaction1.setAmount(10000);
            transaction1.setCategory(categories.get(0)); // 식비
            transaction1.setUser(user1);
            transaction1.setDescription("점심 식사");

            Transaction transaction2 = new Transaction();
            transaction2.setDate(new Date());
            transaction2.setAmount(2500);
            transaction2.setCategory(categories.get(1)); // 교통
            transaction2.setUser(user2);
            transaction2.setDescription("지하철 요금");

            Transaction transaction3 = new Transaction();
            transaction3.setDate(new Date());
            transaction3.setAmount(30000);
            transaction3.setCategory(categories.get(2)); // 패션
            transaction3.setUser(user1);
            transaction3.setDescription("신발 구매");

            transactionRepository.saveAll(List.of(transaction1, transaction2, transaction3));
            System.out.println("Transactions saved");
        } else {
            System.out.println("Data already exists, skipping InnoutRunner execution.");
        }
    }

    private Category createCategory(Long id, String name) {
        Category category = new Category();
        category.setId(id);
        category.setName(name);
        return category;
    }

}