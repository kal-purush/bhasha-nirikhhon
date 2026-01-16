import axios from 'axios';
export const list =data=>axios({
	method:'get',
	url:'v2/movie/in_theaters',
	params:data
}).then((res)=>{
	return res;
})