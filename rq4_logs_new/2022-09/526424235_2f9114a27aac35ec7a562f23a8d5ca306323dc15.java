package el_colibri.controladorfianaciero3.model;

import org.apache.catalina.User;

import java.time.LocalDateTime;

public class Profile {

        private String id;
        private String image;
        private String phone;
        private User user;

        private LocalDateTime createdAt;

        private LocalDateTime  updatedAt;


        public Profile(String id, String image, String phone, User user, LocalDateTime createdAt,
                       LocalDateTime updatedAt) {
            this.id = id;
            this.image = image;
            this.phone = phone;
            this.user = user;
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
        }



        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getImage() {
            return image;
        }

        public void setImage(String image) {
            this.image = image;
        }

        public String getPhone() {
            return phone;
        }

        public void setPhone(String phone) {
            this.phone = phone;
        }

        public User getUser() {
            return user;
        }

        public void setUser(User user) {
            this.user = user;
        }

        public LocalDateTime getCreatedAt() {
            return createdAt;
        }
        public void setCreatedAt(LocalDateTime createdAt) {
            this.createdAt = createdAt;
        }
        public LocalDateTime getUpdatedAt() {
            return updatedAt;
        }
        public void setUpdatedAt(LocalDateTime updatedAt) {this.updatedAt = updatedAt;}

    }


