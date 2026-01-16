/**
 * Copyright 2014 Semen Kiryushin
 *
 * License: Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Created by Semen Kiryushin on 16.08.2015.
 */

class Gost {
	hash(input:ArrayBuffer):ArrayBuffer {
		return null;
	}
}

export function gost(input:ArrayBuffer):ArrayBuffer {
	var g = new Gost();
	return g.hash(input);
}