import { DataTypes, Model } from "sequelize";
import sequelize from "../../config/db";
import User from "./User"; // Import the User model

interface PostAttributes {
  id: typeof DataTypes.UUID;
  title: string;
  description: string;
  attendees: string[]; // This will store an array of User IDs
  creatorId: typeof DataTypes.UUID; // Foreign key for the creator
  location: any; // Sequelize.GEOMETRY("POINT")
  eventDate: Date; // Sequelize.Date
}

interface PostInstance extends Model<PostAttributes>, PostAttributes {}

const Post = sequelize.define<PostInstance>("Post", {
    id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        allowNull: false,
        primaryKey: true,
      },
      title: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      description: {
        type: DataTypes.TEXT,
        allowNull: true,
      },
      attendees: {
        type: DataTypes.ARRAY(DataTypes.UUID),
        defaultValue: [],
        allowNull: true,
      },
      creatorId: {
        type: DataTypes.UUID,
        allowNull: false,
        references: {
          model: "Users",
          key: "id",
        },
      },
      location: {
        type: DataTypes.JSON,
        allowNull: false,
      },
      eventDate: {
        type: DataTypes.DATE,
        allowNull: true,
      },
});

// Define the relationship between Post and User for the creator
Post.belongsTo(User, {
  foreignKey: "creatorId",
  onDelete: "CASCADE",
});

export default Post;