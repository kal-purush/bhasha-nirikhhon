
/**
 * @file      DateRoomConverter.java
 * @brief     to convert date to include in the database$
 * @copyright COPYRIGHT (C) 2018 MITSUBISHI ELECTRIC CORPORATION
 *            ALL RIGHTS RESERVED
 * @author    Vishnu Muraleedharan
 */

package com.example.vichu.roomrecyclerlist.utils;
import android.arch.persistence.room.TypeConverter;

import java.util.Date;


public class DateRoomConverter {

   @TypeConverter
   public static Date toDate(Long value) {
      return value == null ? null : new Date(value);
   }

   @TypeConverter
   public static Long toLong(Date value) {
      return value == null ? null : value.getTime();
   }
}