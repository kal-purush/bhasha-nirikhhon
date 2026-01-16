db.createUser(
        {
            user: "tvt-user",
            pwd: "tvt-password",
            roles: [
                {
                    role: "readWrite",
                    db: "tvt"
                }
            ]
        }
);