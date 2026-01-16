/*
 * Copyright (C) 2016 Clayn <clayn_osmato@gmx.de>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.bplaced.clayn.cfs;

/**
 *
 * @author Clayn <clayn_osmato@gmx.de>
 */
public interface FileAttributes
{
    /**
     * Returns the timestamp of the last time when the file was modified. A file 
     * is modified when something was written to it.
     * @return the last time the file was modified
     * @since 0.3.0
     */
    public long lastModified();
    
    /**
     * Returns the timestamp of the time when the file was created. This time 
     * should only change when the file was deleted and created again. 
     * @return the time the file was created
     * @since 0.3.0
     */
    public long creationTime();
    
    /**
     * Returns the timestamp of the last time when the file was used. Using a file 
     * means it was opened for reading.
     * @return the last time the file was used
     * @since 0.3.0
     */
    public long lastUsed();
}