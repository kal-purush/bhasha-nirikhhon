/**
 * Servicio de chat refactorizado
 * 
 * Versión mejorada del servicio de chat que usa los principios de Clean Architecture
 * separando la lógica en capas y utilizando interfaces de repositorio.
 */

import { Injectable, BadRequestException, NotFoundException } from '@nestjs/common';
import { ChatMessage, ChatRoom, ChatRoomType } from '../../domain/entities';
import { IChatRoomRepository } from '../../domain/repositories/chat-room.repository.interface';
import { ChatDomainService } from '../../domain/services/chat.domain.service';
import { CreateRoomDto } from '../dtos/chat/create-room.dto';
import { CreateMessageDto } from '../dtos/chat/create-message.dto';

@Injectable()
export class ChatService {
  constructor(
    private readonly chatRoomRepository: IChatRoomRepository,
    private readonly chatDomainService: ChatDomainService,
    // Inyectar otros repositorios según sea necesario
  ) {}

  /**
   * Crear una sala de chat
   */
  async createRoom(createRoomDto: CreateRoomDto): Promise<ChatRoom> {
    // Validación y lógica de creación de sala
    // Este método delegaría las validaciones a un servicio de dominio
    // y la persistencia al repositorio
    
    // Ejemplo de validación:
    if (createRoomDto.type === ChatRoomType.SUPPLIER_GROUP && !createRoomDto.supplierId) {
      throw new BadRequestException('Se requiere el ID del proveedor para salas de grupo');
    }
    
    // Creación de la sala a través del repositorio
    return this.chatRoomRepository.create(createRoomDto);
  }

  /**
   * Obtener las salas de chat de un usuario
   */
  async getUserRooms(userId: string, userType: 'employee' | 'visitor'): Promise<ChatRoom[]> {
    return this.chatRoomRepository.findRoomsByUserId(userId, userType);
  }

  /**
   * Obtener mensajes de una sala
   */
  async getRoomMessages(roomId: string, userId: string, userType: 'employee' | 'visitor'): Promise<ChatMessage[]> {
    const room = await this.chatRoomRepository.findById(roomId, true);
    
    if (!room) {
      throw new NotFoundException('La sala de chat no existe');
    }
    
    // Validar acceso usando el servicio de dominio
    const hasAccess = this.chatDomainService.validateRoomAccess(room, userId, userType);
    
    if (!hasAccess) {
      throw new BadRequestException('El usuario no tiene acceso a esta sala de chat');
    }
    
    // Retornar los mensajes de la sala
    // En una implementación real, esto usaría un repositorio de mensajes
    return room.messages || [];
  }

  /**
   * Obtener o crear sala privada entre dos usuarios
   */
  async getOrCreatePrivateRoom(
    user1Id: string,
    user1Type: 'employee' | 'visitor',
    user2Id: string,
    user2Type: 'employee' | 'visitor'
  ): Promise<ChatRoom> {
    // Buscar sala existente
    const existingRoom = await this.chatRoomRepository.findPrivateRoomBetweenUsers(
      user1Id, user1Type, user2Id, user2Type
    );
    
    if (existingRoom) {
      return existingRoom;
    }
    
    // Si no existe, crear una nueva sala
    // Esta lógica utilizaría el repositorio para crear la sala
    // y el servicio de dominio para generar el nombre, etc.
    
    return this.chatRoomRepository.create({
      type: ChatRoomType.PRIVATE,
      name: 'Chat Privado', // En la implementación real usaría el servicio de dominio
      // Añadir otros campos según sea necesario
    });
  }
} 