import { Meteor } from "meteor/meteor";
import { executeQuery } from "./database";
import { User } from "/imports/types/user";
import { Accounts } from "meteor/accounts-base";

Meteor.methods({
    async server_getUsers(): Promise<any> {
        return await executeQuery('SELECT * FROM Users');
    },
    async server_getUserByEmail(email: string): Promise<any> {
        return await executeQuery('SELECT * FROM Users WHERE email = ?', [email]);
    },
    async server_createUser(user: User): Promise<any> {    
        const cleanedEmail = user.email.trim().toLowerCase();
    
        // Utiliser findOneAsync pour rechercher l'utilisateur
        const existingUser = await Meteor.users.findOneAsync({ 'emails.address': cleanedEmail });
        if (existingUser) {
          console.error("Utilisateur existant trouvé :", existingUser);
          throw new Meteor.Error('user-exists', 'Un utilisateur avec cet email existe déjà.');
        }
    
        try {
          // Créer l'utilisateur
          const userId = Accounts.createUser({
            username: user.name,
            email: cleanedEmail,
            password: user.password,
          });

          console.log("Utilisateur créé avec succès, ID :", userId);
          return await executeQuery('INSERT INTO Users (name, email, password) VALUES (?, ?, ?)', [user.name, user.email, user.password]);
        } catch (error) {
          console.error("Erreur lors de la création de l'utilisateur :", error);
          throw new Meteor.Error('server-error', 'Erreur interne lors de la création de l\'utilisateur.');
        }
    }
});