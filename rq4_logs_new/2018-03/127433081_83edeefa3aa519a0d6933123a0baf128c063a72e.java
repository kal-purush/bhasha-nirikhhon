/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sndr.rapidoidchallenge;

import java.util.Random;
import static org.rapidoid.u.U.map;
import static org.rapidoid.setup.On.get;
import org.apache.commons.math3.primes.Primes;
import static java.lang.System.currentTimeMillis;

/**
 * Simple Rapidoid request/response handler to return a json document with a 
 * number and its prime factors
 */
public class Main {

    private static final Random r = new Random(currentTimeMillis());

    public static void main(String[] args) {
        get("/factors").json(() -> {
            int nextNumber = r.nextInt(Integer.MAX_VALUE);
            return map("number", nextNumber, "factors", Primes.primeFactors(nextNumber));
        });
    }

}