/*
 * Copyright 2019, Andreas Becker <andreas AT becker DOT name>
 * 
 * This file is part of The Spring Boot Batch example.
 * 
 * The Spring Boot Batch example is free software: you can redistribute
 * it and/or modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 * 
 * The Spring Boot Batch example is distributed in the hope that it will
 * be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details. You should have received a copy of the GNU
 * General Public License along with The Spring Boot Batch example. If
 * not, see <http://www.gnu.org/licenses/>.
 */

package name.becker.andreas.springbootbatchexample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBootBatchExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootBatchExampleApplication.class, args);
	}

}