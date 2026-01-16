import React from "react";
import { UnauthorizedHeader } from "../header";

export default function Credits() {
  return (
    <div>
      <UnauthorizedHeader />
      <div class="min-h-full max-w-max flex items-center px-6 lg:px-32 " style={{ color: 'white' }}>
          The following are the resources used to make this project possible.
        <section class=" md:w-9/12 xl:w-8/12">
            <p>Create react app at: https://reactjs.org/docs/create-a-new-react-app.html </p>
            <p>Mongoose resource at: https://mongoosejs.com/docs/ </p>
            <p>Chat app tutorial and basic html/css/js code taken from: https://github.com/bradtraversy/chatcord </p>
            <p>Live streaming using Node Media Server at: https://www.npmjs.com/package/node-media-server </p>

        </section>
      </div>
    </div>
  );
}