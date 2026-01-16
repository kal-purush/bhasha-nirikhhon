package org.petmarket.review.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.petmarket.advertisements.advertisement.repository.AdvertisementRepository;
import org.petmarket.errorhandling.ItemNotFoundException;
import org.petmarket.order.repository.OrderRepository;
import org.petmarket.review.dto.RatingList;
import org.petmarket.review.entity.Review;
import org.petmarket.review.repository.ReviewRepository;
import org.petmarket.users.entity.User;
import org.petmarket.users.repository.UserRepository;
import org.petmarket.users.service.UserCacheService;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReviewServiceTest {
    @Mock
    private ReviewRepository reviewRepository;

    @Mock
    private AdvertisementRepository advertisementRepository;

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private UserRepository userRepository;

    @Mock
    private UserCacheService userCacheService;

    @InjectMocks
    private ReviewService reviewService;

    @Test
    public void testFindAllByUser() {
        // Arrange
        User user = new User();
        user.setId(1L);
        Review review = new Review();
        review.setId(1L);
        review.setUser(user);

        when(reviewRepository.findReviewByUserID(anyLong(), anyInt())).thenReturn(Collections.singletonList(review));

        // Act
        List<Review> reviews = reviewService.findAllByUser(user, 10);

        // Assert
        assertEquals(1, reviews.size());
        verify(reviewRepository, times(1)).findReviewByUserID(user.getId(), 10);
    }

    @Test
    public void testFindAllByUserWhenNullUser() {
        // Act
        List<Review> reviews = reviewService.findAllByUser(null, 10);

        // Assert
        assertEquals(0, reviews.size());
        verify(reviewRepository, never()).findReviewByUserID(anyLong(), anyInt());
    }

    @Test
    public void testFindAllByUserWhenSizeSmallerThan0() {
        // Arrange
        User user = new User();
        user.setId(1L);

        // Act
        List<Review> reviews = reviewService.findAllByUser(user, -1);

        // Assert
        assertEquals(0, reviews.size());
        verify(reviewRepository, never()).findReviewByUserID(anyLong(), anyInt());
    }

    @Test
    public void testFindRatingsByUser() {
        // Arrange
        User user = new User();
        user.setId(1L);

        when(reviewRepository.findRatingsByUserID(anyLong()))
                .thenReturn(Collections.singletonList(new Integer[]{0, 0, 0, 0 , 5}));

        // Act
        RatingList ratingList = reviewService.findRatingsByUser(user);

        // Assert
        assertNotNull(ratingList);
        assertEquals(0, ratingList.getRating1());
        assertEquals(0, ratingList.getRating2());
        assertEquals(0, ratingList.getRating3());
        assertEquals(0, ratingList.getRating4());
        assertEquals(5, ratingList.getRating5());
        verify(reviewRepository, times(1)).findRatingsByUserID(user.getId());
    }

    @Test
    public void testFindRatingsByUserWhenNullUser() {
        // Act
        RatingList ratingList = reviewService.findRatingsByUser(null);

        // Assert
        assertNull(ratingList);
    }

    @Test
    public void testDeleteReview() {
        // Arrange
        User user = new User();
        user.setId(1L);
        Review review = new Review();
        review.setId(1L);
        review.setUser(user);

        when(reviewRepository.findById(anyLong())).thenReturn(Optional.of(review));

        // Act
        reviewService.deleteReview(1L);

        // Assert
        verify(reviewRepository, times(1)).delete(review);
        verify(userCacheService, times(1)).evictCaches(user);
    }

    @Test
    public void testDeleteAllReviewsByAdvertisement() {
        // Arrange
        when(advertisementRepository.existsById(anyLong())).thenReturn(true);

        // Act
        reviewService.deleteAllReviewsByAdvertisement(1L);

        // Assert
        verify(reviewRepository, times(1)).deleteAllReviewsByAdvertisementId(1L);
        verify(userCacheService, times(1)).evictCaches();
    }

    @Test
    public void testDeleteAllReviewsByAdvertisementWhenAdvertisementDoesNotExist() {
        // Arrange
        Long advertisementId = 1L;

        when(advertisementRepository.existsById(anyLong())).thenReturn(false);

        // Act and Assert
        assertThrows(ItemNotFoundException.class, () -> reviewService.deleteAllReviewsByAdvertisement(advertisementId));

        verify(advertisementRepository, times(1)).existsById(advertisementId);
        verify(reviewRepository, times(0)).deleteAllReviewsByAdvertisementId(anyLong());
        verify(userCacheService, times(0)).evictCaches();
    }

    @Test
    public void testDeleteAllReviewsByUser() {
        // Arrange
        when(userRepository.existsById(anyLong())).thenReturn(true);

        // Act
        reviewService.deleteAllReviewsByUser(1L);

        // Assert
        verify(reviewRepository, times(1)).deleteAllReviewsByUserId(1L);
        verify(userCacheService, times(1)).evictCaches();
    }

    @Test
    public void testDeleteAllReviewsByUserWhenUserDoesNotExist() {
        // Arrange
        Long userId = 1L;

        when(userRepository.existsById(anyLong())).thenReturn(false);

        // Act and Assert
        assertThrows(ItemNotFoundException.class, () -> reviewService.deleteAllReviewsByUser(userId));

        verify(userRepository, times(1)).existsById(userId);
        verify(reviewRepository, times(0)).deleteAllReviewsByUserId(anyLong());
        verify(userCacheService, times(0)).evictCaches();
    }


    @Test
    public void testDeleteAllReviewsByOrder() {
        // Arrange
        when(orderRepository.existsById(anyLong())).thenReturn(true);

        // Act
        reviewService.deleteAllReviewsByOrder(1L);

        // Assert
        verify(reviewRepository, times(1)).deleteAllReviewsByOrderId(1L);
        verify(userCacheService, times(1)).evictCaches();
    }

    @Test
    public void testDeleteAllReviewsByOrderWhenOrderDoesNotExist() {
        // Arrange
        Long orderId = 1L;

        when(orderRepository.existsById(anyLong())).thenReturn(false);

        // Act and Assert
        assertThrows(ItemNotFoundException.class, () -> reviewService.deleteAllReviewsByOrder(orderId));

        verify(orderRepository, times(1)).existsById(orderId);
        verify(reviewRepository, times(0)).deleteAllReviewsByOrderId(anyLong());
        verify(userCacheService, times(0)).evictCaches();
    }

    @Test
    public void testExistsByAuthorIdAndUserId() {
        // Arrange
        User user = new User();
        user.setId(1L);
        Review review = new Review();
        review.setId(1L);
        review.setUser(user);

        when(reviewRepository.findReviewByAuthorIdAndUserId(anyLong(), anyLong()))
                .thenReturn(Collections.singletonList(review));

        // Act
        boolean exists = reviewService.existsByAuthorIdAndUserId(1L, 1L);

        // Assert
        assertTrue(exists);
    }
}