import { client } from "../config/sanity";
import groq from "groq";

export const getAttempts = async () => {
  try {
    const query = groq`
        *[_type=="attempt"]
        `;
    const data = await client.fetch(query);
    return data;
  } catch (e: any) {
    console.log(e);
    return null;
  }
};

export const getYoutubeLinks = async () => {
  const query = groq`
        *[_type=="youtube"] | order(_createdAt desc)
        `;
  return new Promise((resolve, reject) => {
    client
      .fetch(query)
      .then((data) => {
        resolve(data);
      })
      .catch((e) => {
        reject(e);
      });
  });
};

export const getPodcastLinks = async () => {
  const query = groq`
        *[_type=="podcast"] | order(_createdAt desc)
        `;
  return new Promise((resolve, reject) => {
    client
      .fetch(query)
      .then((data) => {
        resolve(data);
      })
      .catch((e) => {
        reject(e);
      });
  });
};

export const getEvents = async () => {
  const query = groq`
        *[_type=="event"] | order(_createdAt desc)
        `;
  return new Promise((resolve, reject) => {
    client
      .fetch(query)
      .then((data) => {
        resolve(data);
      })
      .catch((e) => {
        reject(e);
      });
  });
};

export const getArticles = async () => {
  const query = groq`
        *[_type=="article"] | order(_createdAt desc)
        `;
  return new Promise((resolve, reject) => {
    client
      .fetch(query)
      .then((data) => {
        resolve(data);
      })
      .catch((e) => {
        reject(e);
      });
  });
};

export const getContributors  = async () => {
  const query = groq`
        *[_type=="contributor"] | order(_createdAt desc)
        `;
  return new Promise((resolve, reject) => {
    client
      .fetch(query)
      .then((data) => {
        resolve(data);
      })
      .catch((e) => {
        reject(e);
      });
  });
}