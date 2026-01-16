import { DataSource } from "typeorm";
import { Seeder, SeederFactoryManager } from "typeorm-extension";
import { Chat } from "../entity/Chat";

export class ChatSeeder implements Seeder {
   async run(
      dataSource: DataSource,
      factoryManager: SeederFactoryManager
   ): Promise<any> {
      const chatRepository = dataSource.getRepository(Chat);

      const chatData = [
         {
            user_id: 1,
            publication_id: 1
         },
         {
            user_id: 2,
            publication_id: 1
         },
         {
            user_id: 1,
            publication_id: 2
         },
      ];

      const chats = chatRepository.create(chatData);
      await chatRepository.save(chats);
   }
}