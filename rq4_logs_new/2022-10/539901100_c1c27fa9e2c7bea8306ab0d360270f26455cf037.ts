import fs from "fs";
import path from "path";
import matter from "gray-matter";
import { remark } from "remark";
import html from "remark-html";

const eventsDir = path.join(process.cwd(), "events");

export type EventData = {
  image: string,
	title: string,
  date?: number,
	position?: boolean,
  text: string,
  content: string,
}


export const getAllEvents = () => {
  const eventNames = fs.readdirSync(eventsDir);

  return eventNames.map((eventName) => {
    return {
      params : {
        event: eventName.replace(/\.md$/, ""),
      }
    }
  });
}

export const getEventData = async (name: string | string[]) : Promise<EventData> => {
  const fileContents = fs.readFileSync(path.join(eventsDir, `${name}.md`), "utf8");
  // Use matter to split the metadata from the content in the .md file
  const matterConversed = matter(fileContents);

  const htmlContent = await remark()
    .use(html)
    .process(matterConversed.content);


  return <EventData> {
    ...matterConversed.data,
    text : "Lorem ipsum dolor sit amet consectetur, adipisicing elit. Vero dolorum harum aspernatur voluptates, ipsam mollitia laudantium? Amet fugiat perferendis, animi atque error maiores enim a cum magnam voluptatem. Repellat, vero!",
    content: htmlContent.toString(),
  }
};

export const getAllEventsData = async () : Promise<Array<EventData>> => {
  const eventNames = fs.readdirSync(eventsDir);
  let allEventsData = eventNames.map(async (eventName) : Promise<EventData> => { 
    return <EventData> (await getEventData(eventName.replace(/\.md$/, "")));
  });
  
  return await Promise.all(allEventsData);
}

export const getAllEventDataSorted = async () : Promise<Array<EventData>> => {
  return (await getAllEventsData()).sort((a, b) => {
    if((a.date || (new Date()).getTime()) < (b.date || (new Date()).getTime())) return 1;
    else return -1;
  });
}