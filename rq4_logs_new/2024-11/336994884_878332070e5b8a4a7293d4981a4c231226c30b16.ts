import revalidate from "@/config/revalidate"
import axios from "axios"
import qs from "qs"

// test token strapi v 5 

const queryString = qs.stringify(
  {
    fields: [ 'slug', 'studyField',]
  },
  {
    encodeValuesOnly: true,
    skipNulls: true
  }
)
export const getStaticPathsBlogs = async () => {
try {
  const response = await axios.get(`http://localhost:1338/api/blogs?${queryString}`, {
    headers: {
      'Authorization': `Bearer ${process.env.STRAPI_BEARER}`, // Замените на ваш токен
    },
  })

  console.log(response, 'response');
  
  
  return {
    paths: Array.from(
      new Set(
        response.data?.data?.map(program => ({
          params: {
            // studyFieldSlug: program?.studyFieldSlug || 'studyFieldSlug',
            slug: program?.slug || 'program'
          }
        }))
      )
    ) || [{ params: { slug: 'program' } }],
    fallback: 'blocking'
  }
}catch(e){
  console.log(e)}

}