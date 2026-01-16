package com.example.demo.repository;
import java.util.List;
import com.example.demo.domain.RefreshToken;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface RefreshTokenRepository extends JpaRepository<RefreshToken, Long> {
    Optional<RefreshToken> findByToken(String token);
    void deleteByUsername(String username);
    
    // 주어진 username 으로 저장된 모든 리프레시 토큰을 조회
    List<RefreshToken> findAllByUsername(String username);
    // 주어진 username 으로 저장된 모든 리프레시 토큰을 한 번에 삭제
    void deleteAllByUsername(String username);
}