/*
 * Copyright (C) 2022 liraline
 * Copyright (C) 2022 luisfelipe3d
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

package br.unicap.meow.compiler.model;

public class Variable {
    private int scope;
    private String name;
    private String type;

    public Variable(int scope, String name, String type) {
        this.scope = scope;
        this.name = name;
        this.type = type;
    }

    public int getScope() {
        return scope;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }
}