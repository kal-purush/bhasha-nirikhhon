package org.petmarket.message.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.petmarket.errorhandling.AccessDeniedException;
import org.petmarket.message.dto.MessageRequestDto;
import org.petmarket.message.entity.Message;
import org.petmarket.users.entity.User;
import org.petmarket.users.service.UserService;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageAccessCheckerServiceTest {
    @Mock
    private UserService userService;

    @Mock
    private MessageService messageService;

    @InjectMocks
    private MessageAccessCheckerService messageAccessCheckerService;


    @Test
    void testCheckCreateAccess_AccessDenied() {
        // Arrange
        User currentUser = new User();
        currentUser.setId(1L);
        when(userService.getCurrentUser()).thenReturn(currentUser);
        MessageRequestDto messageRequestDto = new MessageRequestDto();
        messageRequestDto.setRecipientId(1L);

        // Act And Assert
        assertThrows(AccessDeniedException.class,
                () -> messageAccessCheckerService.checkCreateAccess(messageRequestDto));
    }

    @Test
    void testCheckCreateAccess_AccessGranted() {
        // Arrange
        User currentUser = new User();
        currentUser.setId(1L);
        when(userService.getCurrentUser()).thenReturn(currentUser);
        MessageRequestDto messageRequestDto = new MessageRequestDto();
        messageRequestDto.setRecipientId(2L);

        // Act And Assert
        assertDoesNotThrow(() -> messageAccessCheckerService.checkCreateAccess(messageRequestDto));
    }

    @Test
    void testCheckUpdateAccess_AccessDenied() {
        // Arrange
        User currentUser = new User();
        currentUser.setId(1L);
        when(userService.getCurrentUser()).thenReturn(currentUser);
        when(userService.isCurrentUserAdmin()).thenReturn(false);

        Message message = new Message();
        message.setId(1L);
        message.setAuthor(User.builder().id(2L).build());
        List<Message> messages = List.of(message);

        // Act And Assert
        assertThrows(AccessDeniedException.class, () -> messageAccessCheckerService.checkUpdateAccess(messages));
    }

    @Test
    void testCheckUpdateAccess_AccessGranted() {
        // Arrange
        User currentUser = new User();
        currentUser.setId(1L);
        when(userService.getCurrentUser()).thenReturn(currentUser);
        when(userService.isCurrentUserAdmin()).thenReturn(false);

        Message message = new Message();
        message.setId(1L);
        message.setAuthor(currentUser);
        List<Message> messages = List.of(message);

        // Act And Assert
        assertDoesNotThrow(() -> messageAccessCheckerService.checkUpdateAccess(messages));
    }

    @Test
    void testCheckUpdateAccess_AdminUser() {
        // Arrange
        when(userService.isCurrentUserAdmin()).thenReturn(true);

        Message message = new Message();
        message.setId(1L);
        message.setAuthor(User.builder().id(2L).build());
        List<Message> messages = List.of(message);

        // Act And Assert
        assertDoesNotThrow(() -> messageAccessCheckerService.checkUpdateAccess(messages));
    }

    @Test
    void testCheckReadAccess_AccessDenied() {
        // Arrange
        User currentUser = new User();
        currentUser.setId(1L);
        when(userService.getCurrentUser()).thenReturn(currentUser);
        when(userService.isCurrentUserAdmin()).thenReturn(false);

        Message message = new Message();
        message.setId(1L);
        message.setRecipient(User.builder().id(2L).build());
        when(messageService.getMessageById(1L)).thenReturn(message);

        // Act And Assert
        assertThrows(AccessDeniedException.class, () -> messageAccessCheckerService.checkReadAccess(1L));
    }

    @Test
    void testCheckReadAccess_AccessGranted() {
        // Arrange
        User currentUser = new User();
        currentUser.setId(1L);
        when(userService.getCurrentUser()).thenReturn(currentUser);
        when(userService.isCurrentUserAdmin()).thenReturn(false);

        Message message = new Message();
        message.setId(1L);
        message.setRecipient(currentUser);
        when(messageService.getMessageById(1L)).thenReturn(message);

        // Act And Assert
        assertDoesNotThrow(() -> messageAccessCheckerService.checkReadAccess(1L));
    }

    @Test
    void testCheckReadAccess_AdminUser() {
        // Arrange
        when(userService.isCurrentUserAdmin()).thenReturn(true);

        Message message = new Message();
        message.setId(1L);
        message.setRecipient(User.builder().id(2L).build());

        // Act And Assert
        assertDoesNotThrow(() -> messageAccessCheckerService.checkReadAccess(1L));
    }

    @Test
    void testCheckDeleteAccess_AccessDenied() {
        // Arrange
        User currentUser = new User();
        currentUser.setId(1L);
        when(userService.getCurrentUser()).thenReturn(currentUser);
        when(userService.isCurrentUserAdmin()).thenReturn(false);

        Message message = new Message();
        message.setId(1L);
        message.setAuthor(User.builder().id(2L).build());
        when(messageService.getMessageById(1L)).thenReturn(message);

        // Act And Assert
        assertThrows(AccessDeniedException.class, () -> messageAccessCheckerService.checkDeleteAccess(1L));
    }

    @Test
    void testCheckDeleteAccess_AccessGranted() {
        // Arrange
        User currentUser = new User();
        currentUser.setId(1L);
        when(userService.getCurrentUser()).thenReturn(currentUser);
        when(userService.isCurrentUserAdmin()).thenReturn(false);

        Message message = new Message();
        message.setId(1L);
        message.setAuthor(currentUser);
        when(messageService.getMessageById(1L)).thenReturn(message);

        // Act And Assert
        assertDoesNotThrow(() -> messageAccessCheckerService.checkDeleteAccess(1L));
    }

    @Test
    void testCheckDeleteAccess_AdminUser() {
        // Arrange
        when(userService.isCurrentUserAdmin()).thenReturn(true);

        Message message = new Message();
        message.setId(1L);
        message.setAuthor(User.builder().id(2L).build());

        // Act And Assert
        assertDoesNotThrow(() -> messageAccessCheckerService.checkDeleteAccess(1L));
    }
}