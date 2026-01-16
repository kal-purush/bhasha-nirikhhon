function duplicate(ray){
	//make copy of array
	//return copy concatted with original array
	var ray2 = ray.slice();
	return ray.concat(ray2);
}