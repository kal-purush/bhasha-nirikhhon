/*
 *  Sakado, an app for school
 *  Copyright (c) 2017-2018 Adrien 'Litarvan' Navratil
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package fr.litarvan.sakado.server.pronote;

import java.util.Calendar;

public class Note
{
    private String subject;
    private String note;
    private String date;

    public Note()
    {
    }

    public Note(String subject, String note, String date)
    {
        this.subject = subject;
        this.note = note;
        this.date = date;
    }

    public String getSubject()
    {
        return subject;
    }

    public String getNote()
    {
        return note;
    }

    public Calendar getDate()
    {
        Calendar calendar = Calendar.getInstance();
        String[] split = this.date.split("/");

        calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(split[0]));
        calendar.set(Calendar.MONTH, Integer.parseInt(split[1]));

        return calendar;
    }
}