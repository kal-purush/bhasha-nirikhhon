import { Request, Response } from 'express';
import { AppDataSource } from '../../data-source';
import { Chat } from '../../entity/Chat';
import { Messages } from '../../entity/Messages';
import { Publication } from '../../entity/Publication';
import { User } from '../../entity/User';

export class MessagesController {
	private chatRepository = AppDataSource.getRepository(Chat);
	private messagesRepository = AppDataSource.getRepository(Messages);
	private publicationRepository = AppDataSource.getRepository(Publication);
	private userRepository = AppDataSource.getRepository(User);

	// public save = async (request: Request, response: Response) => {
	// 	try {
	// 		const { username, message, publication_slug } = request.body;
	// 		console.log(
	// 			'SAVE: username, message, publication_slug = ',
	// 			username,
	// 			message,
	// 			publication_slug
	// 		);
	// 		const user = await this.userRepository.findOne({
	// 			where: { name: username },
	// 		});
	// 		const user_id = user?.id;
	// 		console.log('SAVE: user_id = ', user_id);
	// 		const publication = await this.publicationRepository.findOne({
	// 			where: { slug: publication_slug },
	// 		});
	// 		const publication_id = publication?.id;
	// 		console.log('SAVE: publication_id = ', publication_id);
	// 		const chat = await this.chatRepository.findOne({
	// 			where: { publication_id, user_id },
	// 		});
	// 		console.log('SAVE: chat = ', chat);
	// 		const newMessage = {
	// 			role: 'user',
	// 			content: message,
	// 			chat_id: chat?.id,
	// 		};
	// 		console.log('SAVE: newMessage = ', newMessage);
	// 		await this.messagesRepository.save(newMessage);
	// 		return response.status(201).json(newMessage);
	// 	} catch (error) {
	// 		return response.status(500).json({
	// 			message: 'An error occurred while saving a message: ',
	// 			error: error.message,
	// 		});
	// 	}
   // };
   
   public save = async (request: Request, response: Response) => {
	try {
		const { username, message, publication_slug } = request.body;
		const user = await this.userRepository.findOne({
			where: { name: username },
		});
		const publication = await this.publicationRepository.findOne({
			where: { slug: publication_slug },
		});
		let chat = await this.chatRepository.findOne({
			where: { publication_id: publication?.id, user_id: user?.id },
		});
		if (!chat) {
			// create a new chat entity if one does not exist
			chat = new Chat();
			chat.user_id = user?.id;
			chat.publication_id = publication?.id;
			await this.chatRepository.save(chat);
		}
		const newMessage = new Messages();
		newMessage.role = 'user';
		newMessage.content = message;
		newMessage.chat = chat;
		await this.messagesRepository.save(newMessage);
		return response.status(201).json(newMessage);
	} catch (error) {
		return response.status(500).json({
			message: 'An error occurred while saving a message: ',
			error: error.message,
		});
	}
};

}