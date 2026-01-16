/* eslint-disable no-underscore-dangle */
const express = require('express');
const fs = require('fs');
const AdmZip = require('adm-zip');
const monitorLogService = require('../services/monitorLog.service');
const segmentService = require('../services/section.segment.service');

const router = express.Router();

const generalHeaders = [
  'Surveyor Name',
  'Survey Segment',
  'Date',
  'Survey Start Time',
  'Survey End Time',
  'Temperature (F)',
  'Cloud Cover (%)',
  'Precipitation',
  'Wind (Speed/Direction)',
  'Tides(ft)',
  'Overall Habitat Type',
  'Habitat Width (ft)',
  'Partners',
];

const listedAnimalHeaders = [
  'Map #',
  'Adults',
  'Fledges',
  'Chicks',
  'Time',
  'Habitat Description',
  'GPS',
  'Cross Street/Towers',
  '# of Male Adults',
  '# of Male Fledges',
  '# of Male Chicks',
  '# of Female Adults',
  '# of Female Fledges',
  '# of Female Chicks',
  'Nest & Eggs',
  'Behaviors Observed',
  'Banding Code',
  'Accuracy Confidence',
  'Additional Notes',
];

const nonListedHeaders = ['Species', 'Total', 'Notes'];

const humanHeaders = [
  'Beach Activity',
  'Water Activity',
  'Airborne Activity',
  '[Speeding] Motorized Vehicles',
  '[Non-Speeding] Motorized Vehicles',
  '[Off Leash] Domestic Animals',
  '[On Leash] Domestic Animals',
  'Outreach',
  'Other Notes',
];

const disclaimer =
  'OC Habitats strives to survey as many of the segments as possible each month. Some segments may not have been surveyed due to volunteer cancellation (due to illness, weather, or some other reason). Some segments are regularly not getting surveyed due to access issues (parking or land structures).  Some segments get more attention than others since we are aware the SNPL use these segments more often or there are issues with these segments that need more regular attention. ';

const reportsDirectory = `${__dirname}\\..\\reports`;
// const convertToPascal = (text) => {
//   return text.replace(/((?<!^)[A-Z](?![A-Z]))(?=\S)/g, ' $1').replace(/^./, (s) => s.toUpperCase());
// };

const joinValues = (valuesList) => {
  let row = '';
  let size;
  if (Array.isArray(valuesList)) {
    valuesList.forEach((value) => {
      size = Object.values(value).length;
      Object.values(value).forEach((entry, index) => {
        if (index === size - 2) row += `${entry}\r\n`;
        else if (index !== size - 1) {
          row += `${entry},`;
        }
      });
    });
  } else {
    // const size = Object.values(valuesList).length;
    size = Object.values(valuesList).length;
    Object.values(valuesList).forEach((entry, index) => {
      if (index === size - 2) row += `${entry}\r\n`;
      else if (index !== size - 1) {
        row += `${entry},`;
      }
    });
  }

  return row;
};

const joinKeys = (keysList) => {
  let row = '';
  // let size;
  // if (Array.isArray(keysList)) {
  //   keysList.forEach((key) => {
  //     size = Object.keys(key).length;
  //     Object.keys(key).forEach((entry, index) => {
  //       if (index === size - 2) row += `${entry}\r\n`;
  //       else if (index !== size - 1) {
  //         row += `${entry},`;
  //       }
  //     });
  //   });
  // } else {
  const size = Object.keys(keysList).length;
  Object.keys(keysList).forEach((entry, index) => {
    if (index === size - 2) row += `${entry}\r\n`;
    else if (index !== size - 1) {
      row += `${entry},`;
    }
  });
  // }
  return row;
};

const formatArray = (ar) => {
  return `"${ar}"`;
};

const convertToCSV = (submission) => {
  // Append General Information Headers
  let csv = `General Information\r\n${generalHeaders.join(',')}`;
  let additionalString = '';

  // Append Additional General Information Headers
  if (submission.generalAdditionalFieldValues) {
    csv += joinKeys(submission.generalAdditionalFieldValues);
    additionalString = joinValues(submission.generalAdditionalFieldValues);
  }

  // Append General Information Fields
  csv += `\r\n${submission.submitter.firstName} ${submission.submitter.lastName},${
    submission.segment.segmentId
  },${new Date(submission.date).toDateString()},${submission.startTime},${submission.endTime},${
    submission.temperature
  },${submission.cloudCover},${submission.precipitation},${submission.windSpeed} ${
    submission.windDirection
  },${submission.tides},${submission.habitatType},${
    submission.habitatWidth
  },"${submission.sessionPartners.toString()}"`;

  // Append ADDITIONAL General Information Fields
  csv += `,${additionalString}\r\n\r\n`;
  additionalString = '';

  // Append Listed Species, Header, and Fields
  submission.listedSpecies.forEach((entry) => {
    csv += `${entry.species.name}\r\n${listedAnimalHeaders.join(',')}\r\n`;
    entry.entries.forEach((input) => {
      const formattedInput = { ...input.toJSON() };
      delete formattedInput._id;
      let formattedGps = '';
      formattedInput.gps.forEach((set) => {
        const temp = { ...set };
        delete temp._id;
        formattedGps += JSON.stringify(temp).replace(/['"]+/g, '');
      });
      formattedInput.gps = formatArray(formattedGps);
      const codesAr = [];

      formattedInput.bandTabs.forEach((band) => {
        codesAr.push(band.manualCode ? band.manualCode : band.code);
      });

      formattedInput.nesting = formatArray(formattedInput.nesting);
      formattedInput.sex = formattedInput.sex.toString();
      formattedInput.bandTabs = formatArray(codesAr);
      formattedInput.behaviors = formatArray(formattedInput.behaviors);
      csv += joinValues(formattedInput);
    });

    csv += `\r\nInjured ${entry.species.name}\r\n${entry.injuredCount}\r\n\r\n`;
  });

  // Append Non Listed Species Title, Headers, and Fields
  csv += `Non-Listed Species\r\n${nonListedHeaders.join(',')}\r\n`;
  submission.additionalSpecies.entries.forEach((addSpecie) => {
    csv += `${addSpecie.species.name},${addSpecie.count},${addSpecie.notes}\r\n`;
  });

  csv += `\r\nInjured Terrestrial Wildlife\r\n${submission.additionalSpecies.injuredCount}\r\n`;

  // // Append Predator Title + Headers
  const predatorHeaders = [];
  const predatorCount = [];
  submission.predators.forEach((predator) => {
    predatorHeaders.push(predator.species.name);
    predatorCount.push(predator.count);
  });
  csv += `\r\n${predatorHeaders},Other predators(s)\r\n`;
  csv += `${predatorCount},${submission.predatorOthers || ''}\r\n`;

  // Append Human Activity Title + Headers
  csv += `\r\nHuman Activity\r\n${humanHeaders.join(',')}`;
  // // Append Additional Human Activity Headers
  // if (submission.humanActivityAdditionalFieldValues) {
  //   csv += joinKeys(submission.humanActivityAdditionalFieldValues.toJSON());
  //   additionalString = joinValues(submission.humanActivityAdditionalFieldValues.toJSON());
  // }
  // // Append Human Activity Fields
  // csv += '\r\n';
  // csv += joinValues(submission.humanActivityEntries.toJSON());
  // // Append Additional Human Activity Fields
  // csv += additionalString;

  csv += '\r\n\r\nSegment Totals\r\n';

  return csv;
};

const getSegmentNames = async () => {
  const segments = await segmentService.getSegNames().then((dict) => {
    return dict.map((segment) => {
      return segment.toJSON().segmentId;
    });
  });
  return segments;
};

const getSegmentRow = async (submissions) => {
  let segmentString = '';
  const segments = await getSegmentNames();
  const segmentDictionary = Object.assign({}, ...segments.map((segment) => ({ [segment]: 0 })));
  submissions.forEach((submission) => {
    if (submission.segment.segmentId in segmentDictionary) {
      segmentDictionary[submission.segment.segmentId] += 1;
    }
  });
  segmentString += joinKeys(segmentDictionary);
  segmentString += joinValues(segmentDictionary);
  return segmentString;
};

const createCSV = (data, id) => {
  try {
    fs.writeFileSync(`${reportsDirectory}\\report-${id}.csv`, data, { flag: 'wx' });
  } catch (err) {
    console.log('ERR writing csv', err);
  }
};

router.get('/report', async (req, res) => {
  let { startDate, endDate } = req.body;
  startDate = new Date(startDate).setHours(0, 0, 0);
  endDate = new Date(endDate).setHours(23, 59, 59, 999);
  try {
    const reports = await monitorLogService.getSubmissionsByDates(startDate, endDate);
    if (!reports.length) {
      res.status(400).json({ message: `Reports from ${startDate} to  ${endDate} doesn't exist` });
    } else {
      const segStr = await getSegmentRow(reports);

      reports.forEach((report) => {
        let csvData = convertToCSV(report);
        csvData += `${segStr}\r\n${disclaimer}`;
        createCSV(csvData, report._id.toString());
      });
      const zip = new AdmZip();
      fs.readdirSync(reportsDirectory).forEach((file) => {
        zip.addLocalFile(`${reportsDirectory}\\${file}`);
      });
      res.writeHead(200, {
        'Content-Disposition': `attachment; filename="${new Date(
          startDate,
        ).toLocaleDateString()}_${new Date(endDate).toLocaleDateString()}_reports.zip"`,
        'Content-Type': 'application/zip',
      });
      const zipFileContents = zip.toBuffer();
      res.end(zipFileContents);

      // res.status(200).send('good');
    }
  } catch (err) {
    console.error(err);
    res.status(400).json({ error: err });
  } finally {
    fs.readdirSync(reportsDirectory).forEach((file) => {
      fs.unlink(`${reportsDirectory}\\${file}`, (err) => {
        if (err) console.log(err);
      });
    });
  }
});
module.exports = router;