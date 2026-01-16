import './news-article.js';
import {
  topHeadlinesUrl,
} from './WebApiKey.js';

window.addEventListener('load', () => {
  getNews();
  registerSW();
});

async function getNews() {
  const res = await fetch(topHeadlinesUrl);
  const json = await res.json();

  const main = document.querySelector('main');

  json.articles.forEach((article) => {
    const el = document.createElement('news-article');
    el.article = article;
    main.appendChild(el);
  });
}

async function registerSW() {
  if ('serviceWorker' in navigator) {
    try {
      await navigator.serviceWorker.register('./sw.js');
    } catch (e) {
      console.log('SW registration failed');
    }
  }
}


// async function registerSW() {
//   if ('serviceWorker' in navigator) {
//     try {
//       await navigator.serviceWorker.register('./sw.js');
//     } catch (e) {
//       console.log('SW registration failed');
//     }
//   }
// }


// const request = new XMLHttpRequest();

// request.open('GET', 'http://www.recipepuppy.com/api/?i=onions,garlic&q=omelet&p=3', true);


// request.onload = function () {
//   const data = JSON.parse(this.response);

//   if (request.status >= 200 && request.status < 400) {
//     data.forEach((movie) => {
//       console.log(movie);
//     });
//   } else {
//     console.log('error');
//   }
// };


// request.send();

// fetch('https://jsonplaceholder.typicode.com/posts')
// 	.then(function (response) {
// 		if (response.ok) {
// 			return response.json();
// 		} else {
// 			return Promise.reject({
// 				status: response.status,
// 				statusText: response.statusText
// 			});
// 		}
// 	})
// 	.then(function (data) {
// 		console.log('success', data);
// 		return fetch('https://jsonplaceholder.typicode.com/posts/' + data[0].id);
// 	})
// 	.then(function (response) {
// 		return response.json();
// 	})
// 	.then(function (post) {
// 		console.log('success', post);
// 	})
// 	.catch(function (error) {
// 		console.log('error', error);
// 	});