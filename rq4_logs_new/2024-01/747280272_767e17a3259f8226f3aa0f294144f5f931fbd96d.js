"use client";
import { v4 as uuidv4 } from "https://jspm.dev/uuid";

//check local storage for uuid
const scriptTag = document.getElementById("uniqueQRs");

// if (typeof window !== "undefined") {
//   scriptTag = document.getElementById("uniqueQRs");
// }

async function checkQRid(businessname, location) {
  //has the campaign past its end date?

  // i think lambda does this now, need to check
  // const currentDate = new Date()

  // const hasPassed = currentDate.getTime() > savedDate.getTime();
  // hasPassed === false from the if
  // if this does do it im not sure how i was getting the saved date

  //is location the same?
  const storedArray = JSON.parse(window.localStorage.getItem("qrInfo"));

  const foundLocation =
    storedArray?.find((obj) => obj.hasOwnProperty(location)) || null;
  const qrID = foundLocation ? foundLocation[`${location}`] : null;
  if (qrID !== null) {
    //increment the location found with its value(id)
    console.log("has location");
    const response = await fetch(
      `https://b66slqv47rtplrq2n6nxqddmja0jmxqs.lambda-url.us-east-1.on.aws/?scanID=${qrID}&business=${businessname}`
    );
    // const full = await response.json();
    // console.log(full);
  } else {
    //location not found, needs new scan on server
    console.log("no location");
    const newUUID = uuidv4();
    try {
      const firstScan = await fetch(
        `https://b66slqv47rtplrq2n6nxqddmja0jmxqs.lambda-url.us-east-1.on.aws/?scanID=${newUUID}&unique=true&business=${businessname}&from=${location}`
      );
      // const full = await firstScan.json();
      console.log(firstScan);
    } catch (e) {
      console.log(e);
    }

    const locationArray = window.localStorage.getItem("qrInfo") || [];

    const transform = [...locationArray, { [location]: `${newUUID}` }];
    // on success save localUUID to local
    window.localStorage.setItem("qrInfo", JSON.stringify(transform));
    // console.log(newUUID, "end");
  }
}

export async function centexQRCheck() {
  // pathtotrack,
  // searchParams,
  // businessname,
  // location,
  //pathtotrack here is the full url WITH a param value ie https://www.centexqrs.com/howitworks?fromQR=true

  //then path to track is already ONLY from my QRs
  // now the linkTo object should have the params for it as well

  /*
    EXAMPLE
    link = "https://www.centexqrs.com/howitworks?fromQR=true"
    pathtotrack = "/howitworks?fromQR=true"
    businessname = "CentexQRs"
    location = "website"
  */

  if (typeof window !== "undefined") {
    // Create a URLSearchParams object using the current URL
    // const url = new URL(currentUrl);

    const urlParams = new URLSearchParams(new URL(window.location.href).search);

    // Get the value of a specific parameter
    const location = urlParams.get("from");

    var pathtotrack = scriptTag.getAttribute("pathtotrack");
    // var searchParams = location + qrParam
    var businessname = scriptTag.getAttribute("businessname");
    // localQRUUID = window.localStorage.getItem("localQRUUID") || null;
    console.log(
      "we are running on the client",
      // localQRUUID,
      window.location.pathname,
      window.location.search,
      location
    );
    if (
      window.location.pathname === pathtotrack &&
      // window.location.search === searchParams
      location != null
    ) {
      console.log("correct path");
      checkQRid(businessname, location);
    }
  } else {
    // localQRUUID = window.localStorage.getItem("localQRUUID") || null;
    console.log("we are running on the server");
  }

  // if (window.location.pathname === pathtotrack) {
  //   checkQRid(localQRUUID, businessname, location, link);
  // }
}

if (scriptTag != null) {
  centexQRCheck();
}

// centexQRCheck(
//   "/howitworks",
//   "?fromQR=true",
//   "CentexQRs",
//   "website",
//   "https://www.centexqrs.com/howitworks?fromQR=true"
// );

// the above should allow for a script to be called from a cdn
// then in a second script i should be able to email them the instructions;
// all they do is copy paste;
/*
  <script src="locationofmyscript" pathtotrack="location/on/website" businessname="name"/>
*/