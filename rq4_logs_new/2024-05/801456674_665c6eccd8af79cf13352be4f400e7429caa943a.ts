import app from "./app";
import mongoose from "mongoose";
import config from "./app/config";


async function main() {
  try {
    await mongoose.connect(`${config.dbs}`);

    app.listen(config.port, () => {
      console.log(`app is listening on Port ${config.port}`);
    });

  } catch (error) {
    console.log(error);

  }


  


}

main()
// async function bootstrap() {
//   server = app.listen(config.port, () => {
//     console.log(`app is listening on Port ${config.port}`);
//   });
// }



// bootstrap();