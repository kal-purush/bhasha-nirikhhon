import groq from 'groq';
export const homeQuery = groq`*[_type == 'home'][0]{
 'quote':quote{
       'author': author[_key ==$language].value,
         'quote':quote[_key ==$language].value,
     },
         
      'banner':banner{
      'title':  title[_key ==$language].value,
       'dateEvent':dateEvent[_key ==$language].value,
       'img':img.asset ->url

      },
    'winner':winner[]{
    'name': name[_key ==$language].value,
    'champion': champion[_key ==$language].value,
      'img':img.asset ->url

    },
    'news':news[]->{
      'description':description[_key ==$language].value,
      'title':title[_key ==$language].value,
      'img':img.asset->url

    },
    sponsors,
    'videos':videos[]{
      'title':title[_key ==$language].value,
      link
    },
}`;

export const newsQuery = groq`*[_type=='news']{
    _id, 
    'slug':slug.current,
    'description':description[_key ==$language].value,
    'title':title[_key ==$language].value,
    'img':img.asset->url
  }`;
export const currentNewsQuery = groq`*[_type =="news" && slug.current==$slug]{
    _id, 
    'slug':slug.current,
    'description':description[_key ==$language].value,
    'title':title[_key ==$language].value,
    'img':img.asset->url
  }`;