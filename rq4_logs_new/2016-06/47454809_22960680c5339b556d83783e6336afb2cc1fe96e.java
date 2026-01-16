package models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;

public class Event implements Serializable{
		@Id
		@SequenceGenerator(name="EventId", sequenceName="EventIdSequence", allocationSize=1)
		@GeneratedValue(strategy=GenerationType.SEQUENCE, generator ="EventId")
		@Column
		private int id;
		@Column
		private String nameGroupe;
		@Column
		private String watchingDate;
		@Column
		private String adminMessage;
		@Column
		private int idAdmin;
		@Column
		private String creationDate;
		@Column
		private Film film;
		@Column
		private User admin;
		
		private List<User> userInvited = new ArrayList<User>();
		private List<User> userComing = new ArrayList<User>();
		private List<User> userNotComing = new ArrayList<User>();

		public Event() {
			super();
		}
		
		public Event(int id, String nameGroupe, String watchingDate, String adminMessage, int idAdmin,
				String creationDate, Film film, User admin, List<User> userInvited, List<User> userComing,
				List<User> userNotComing) {
			super();
			this.id = id;
			this.nameGroupe = nameGroupe;
			this.watchingDate = watchingDate;
			this.adminMessage = adminMessage;
			this.idAdmin = idAdmin;
			this.creationDate = creationDate;
			this.film = film;
			this.admin = admin;
			this.userInvited = userInvited;
			this.userComing = userComing;
			this.userNotComing = userNotComing;
		}
		
		public Event(int id, String nameGroupe, String watchingDate, String adminMessage, int idAdmin,
				String creationDate, Film film, User admin) {
			super();
			this.id = id;
			this.nameGroupe = nameGroupe;
			this.watchingDate = watchingDate;
			this.adminMessage = adminMessage;
			this.idAdmin = idAdmin;
			this.creationDate = creationDate;
			this.film = film;
			this.admin = admin;
		}
		
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public String getNameGroupe() {
			return nameGroupe;
		}
		public void setNameGroupe(String nameGroupe) {
			this.nameGroupe = nameGroupe;
		}
		public String getWatchingDate() {
			return watchingDate;
		}
		public void setWatchingDate(String watchingDate) {
			this.watchingDate = watchingDate;
		}
		public String getAdminMessage() {
			return adminMessage;
		}
		public void setAdminMessage(String adminMessage) {
			this.adminMessage = adminMessage;
		}
		public int getIdAdmin() {
			return idAdmin;
		}
		public void setIdAdmin(int idAdmin) {
			this.idAdmin = idAdmin;
		}
		public String getCreationDate() {
			return creationDate;
		}
		public void setCreationDate(String creationDate) {
			this.creationDate = creationDate;
		}
		public Film getFilm() {
			return film;
		}
		public void setFilm(Film film) {
			this.film = film;
		}
		public User getAdmin() {
			return admin;
		}
		public void setAdmin(User admin) {
			this.admin = admin;
		}
		public List<User> getUserInvited() {
			return userInvited;
		}
		public void setUserInvited(List<User> userInvited) {
			this.userInvited = userInvited;
		}
		public List<User> getUserComing() {
			return userComing;
		}
		public void setUserComing(List<User> userComing) {
			this.userComing = userComing;
		}
		public List<User> getUserNotComing() {
			return userNotComing;
		}
		public void setUserNotComing(List<User> userNotComing) {
			this.userNotComing = userNotComing;
		}
		public int getNbrOfPeopleInvited(){
			return getNbrOfPeopleComing() + getNbrOfPeopleNotComing() + getNbrOfPeopleNotAnswering();
		}
		public int getNbrOfPeopleComing(){
			return userComing.size();
		}
		public int getNbrOfPeopleNotComing(){
			return userNotComing.size();
		}
		public int getNbrOfPeopleNotAnswering(){
			return userInvited.size();
		}
}