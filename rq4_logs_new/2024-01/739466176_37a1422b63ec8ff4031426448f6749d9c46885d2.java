package com.anghel.music_band_organizer.services.mail;

import com.anghel.music_band_organizer.models.entities.Band;
import com.anghel.music_band_organizer.models.entities.Mail;
import com.anghel.music_band_organizer.models.entities.Rehearsal;
import com.anghel.music_band_organizer.models.entities.User;

public interface MailService {

    void sendMail(String mailTarget, Mail mail, String methodName);

    Mail prepareMailMessage(User user, String methodName);

    Mail prepareMailRehearsal(Rehearsal rehearsal, String methodName);

    Mail prepareMailBand(Band band, String methodName);
}