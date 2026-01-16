import { FeatureCollection, LineString } from "geojson";
import { LayerProps, MapRef } from "react-map-gl";

import { Coords } from "@shared/helios-types";
import { generateFakeTelemetryData } from "@shared/helios-types";

import { TrackList } from "./Map";
import { PacketMarkerData } from "./Map";

// Created from here: https://www.scribblemaps.com/create#/lat=36.0164938&lng=-86.99736354&z=14&t=hybrid
const RANDOM_TRACK = [
  [-87.030258154, 36.017031833],
  [-87.028884863, 36.0149491],
  [-87.028198217, 36.012449748],
  [-87.027854894, 36.008631142],
  [-87.026824926, 36.003423654],
  [-87.020130132, 35.998076941],
  [-87.010860418, 35.994743742],
  [-87.006161188, 35.992851394],
  [-87.001869653, 35.990351343],
  [-86.99526069, 35.987573415],
  [-86.991312479, 35.986323315],
  [-86.987793421, 35.988754046],
  [-86.983587717, 35.990351343],
  [-86.980068659, 35.991670824],
  [-86.977837061, 35.994031947],
  [-86.975862955, 35.9979207],
  [-86.975948786, 36.001670388],
  [-86.977322077, 36.005905933],
  [-86.97663543, 36.010280108],
  [-86.97903869, 36.011876968],
  [-86.981356118, 36.014306912],
  [-86.982471918, 36.017673996],
  [-86.980068658, 36.020312022],
  [-86.977150415, 36.021457454],
  [-86.974961732, 36.022811124],
  [-86.974918817, 36.024893649],
  [-86.974489664, 36.027982625],
  [-86.975648378, 36.030238542],
  [-86.976678346, 36.033396719],
  [-86.979467844, 36.034646072],
  [-86.982686494, 36.033951989],
  [-86.986291383, 36.032806739],
  [-86.990368341, 36.03204323],
  [-86.99397323, 36.031175597],
  [-86.997535203, 36.029960895],
  [-87.003972505, 36.028468521],
  [-87.011010622, 36.02721907],
  [-87.021052811, 36.026455506],
  [-87.025902245, 36.022602869],
  [-87.02761886, 36.020242603],
  [-87.030258154, 36.017031833],
];
const CORVETTE_RACE_LOOP_GEO_JSON = {
  features: [
    {
      geometry: { coordinates: [-86.366554059, 37.001949324], type: "Point" },
      id: "sma8d7c840",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.365435577, 37.002456989], type: "Point" },
      id: "smab153bbe",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363890624, 37.003228119], type: "Point" },
      id: "sm8578d706",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363490975, 37.003408048], type: "Point" },
      id: "sm05d3c772",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.362005031, 37.004082779], type: "Point" },
      id: "sm80252109",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361562466, 37.004284126], type: "Point" },
      id: "sm63e0a8aa",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360674654, 37.004706096], type: "Point" },
      id: "sm43397ea5",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360446667, 37.004901015], type: "Point" },
      id: "sm516e33e4",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360296463, 37.005661412], type: "Point" },
      id: "sm8a70ed12",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360532497, 37.006061955], type: "Point" },
      id: "sm3dbcc19e",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361245964, 37.00633398], type: "Point" },
      id: "smd4737986",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.362093542, 37.006458212], type: "Point" },
      id: "sm37392a14",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.362892841, 37.006286858], type: "Point" },
      id: "sm6ae93ffb",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363431965, 37.005901309], type: "Point" },
      id: "sma9e3556a",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363622402, 37.005595011], type: "Point" },
      id: "sm29a2d2bf",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363815521, 37.00516662], type: "Point" },
      id: "smbf8a5f6e",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363989864, 37.004731801], type: "Point" },
      id: "sm7705d7c5",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.364376103, 37.004453344], type: "Point" },
      id: "sm6b4d73d0",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.364807939, 37.004294837], type: "Point" },
      id: "smb658f2c4",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.365714526, 37.003964971], type: "Point" },
      id: "sm2dd39752",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.36638776, 37.003915705], type: "Point" },
      id: "sm87ea9ebd",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.367106592, 37.004181312], type: "Point" },
      id: "sm429c973f",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368082916, 37.004474765], type: "Point" },
      id: "sm58004be1",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368809795, 37.004453345], type: "Point" },
      id: "sm91106639",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369558131, 37.004119195], type: "Point" },
      id: "smb023c2ac",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370388273, 37.003292383], type: "Point" },
      id: "smc80dee95",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370799992, 37.002710823], type: "Point" },
      id: "sme63b8a31",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371360573, 37.00235096], type: "Point" },
      id: "sm7f248788",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372090134, 37.002023227], type: "Point" },
      id: "smcfaffc6f",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372795556, 37.001449155], type: "Point" },
      id: "sma4e5bc35",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.373345409, 37.00084509], type: "Point" },
      id: "sme33e110e",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.374302957, 36.999827593], type: "Point" },
      id: "sm575f899a",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.37434319, 36.999529839], type: "Point" },
      id: "sm6811d3ce",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.374233219, 36.999176388], type: "Point" },
      id: "sm73035860",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.374131295, 36.998996449], type: "Point" },
      id: "smd10aa560",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.373718235, 36.998960033], type: "Point" },
      id: "smad1f16ab",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372723135, 36.999146399], type: "Point" },
      id: "sm72446d50",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371910426, 36.999469859], type: "Point" },
      id: "sm84f8909a",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369998011, 37.000328845], type: "Point" },
      id: "sma01982a4",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368351134, 37.001110705], type: "Point" },
      id: "sm87a4647a",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.366554059, 37.001949324], type: "Point" },
      id: "sma8d7c840",
      properties: {},
      type: "Feature",
    },
  ],
  id: "root",
  type: "FeatureCollection",
};
const GRAND_MAX_STRAIGHT_GEO_JSON = {
  features: [
    {
      geometry: { coordinates: [-86.360500311, 37.004912795], type: "Point" },
      id: "sm45a8ac64",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360390341, 37.005356181], type: "Point" },
      id: "sm04eb3d03",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360344743, 37.005645345], type: "Point" },
      id: "sm465d5bbd",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.36043862, 37.005945216], type: "Point" },
      id: "sm42d287ef",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360819493, 37.006125139], type: "Point" },
      id: "sma0ecb2c4",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361404215, 37.006365035], type: "Point" },
      id: "sm74d18684",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.362300073, 37.006446429], type: "Point" },
      id: "sm52a30982",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.362911616, 37.006262223], type: "Point" },
      id: "smb17c50e8",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363415872, 37.00589381], type: "Point" },
      id: "smf0bb72ef",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363786017, 37.005251225], type: "Point" },
      id: "sm9ac9ffed",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.364220534, 37.00460435], type: "Point" },
      id: "smc9c2ad47",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.364719425, 37.004171671], type: "Point" },
      id: "sm94cca9a9",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.365277324, 37.003691863], type: "Point" },
      id: "sm641fc855",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.365845952, 37.003130656], type: "Point" },
      id: "sm046db624",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.366473589, 37.002629422], type: "Point" },
      id: "sm7a3043c1",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368034635, 37.001926832], type: "Point" },
      id: "sm54c39eea",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368689093, 37.001665502], type: "Point" },
      id: "sm6991b5aa",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369606408, 37.001691207], type: "Point" },
      id: "sm7ad11996",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370078477, 37.001695491], type: "Point" },
      id: "smedf14920",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370443257, 37.001348477], type: "Point" },
      id: "sm67431388",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370743665, 37.001091428], type: "Point" },
      id: "smaf2eee8b",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371446404, 37.000778684], type: "Point" },
      id: "sm9a5158f7",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371945295, 37.000555907], type: "Point" },
      id: "smb584eea4",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372192058, 37.000598749], type: "Point" },
      id: "sm427af187",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372256431, 37.000855799], type: "Point" },
      id: "smcd26cfbd",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372127685, 37.001151406], type: "Point" },
      id: "sma7945d34",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371811184, 37.001545547], type: "Point" },
      id: "sm1c38f1d4",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371280107, 37.001832583], type: "Point" },
      id: "sm4ea18f36",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370362791, 37.00211105], type: "Point" },
      id: "smdb30d270",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369971189, 37.002380947], type: "Point" },
      id: "sm9c482eac",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369702968, 37.002903603], type: "Point" },
      id: "sm2d668b8e",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369381103, 37.003353427], type: "Point" },
      id: "sm43ee265f",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368785652, 37.00345196], type: "Point" },
      id: "sm7eeb1071",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368383321, 37.003254895], type: "Point" },
      id: "sm0fe86ac3",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.367879066, 37.002916455], type: "Point" },
      id: "sme40c2b8d",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.367546472, 37.002903603], type: "Point" },
      id: "sm873274a6",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.366940292, 37.00316493], type: "Point" },
      id: "sme62aa6c3",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.36650041, 37.00366616], type: "Point" },
      id: "sm8ec06207",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.366505775, 37.003974608], type: "Point" },
      id: "smaf8b045c",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.367691311, 37.004313043], type: "Point" },
      id: "smb5843186",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368447694, 37.004467266], type: "Point" },
      id: "sma2e94d94",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369000229, 37.004394438], type: "Point" },
      id: "smb91bcaf6",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369456204, 37.004175956], type: "Point" },
      id: "sm7f70cc04",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369922909, 37.003691865], type: "Point" },
      id: "smfc42c285",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370829495, 37.002663696], type: "Point" },
      id: "sma6c5c962",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372068676, 37.001948254], type: "Point" },
      id: "sm2f1dfe11",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372841153, 37.001271363], type: "Point" },
      id: "smfd15f9d6",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.374214444, 36.999823309], type: "Point" },
      id: "smb46027b1",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.37438074, 36.999564113], type: "Point" },
      id: "sm0e9e66c5",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.374321731, 36.999242794], type: "Point" },
      id: "sm83364f72",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.374192985, 36.999009302], type: "Point" },
      id: "sm09cb4e0c",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.373747739, 36.998990023], type: "Point" },
      id: "sme1d98c0c",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.37298063, 36.999154968], type: "Point" },
      id: "sm496a41a7",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372288617, 36.999343474], type: "Point" },
      id: "sm197bb154",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371164771, 36.999844729], type: "Point" },
      id: "smbdcde48f",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.36983976, 37.000433808], type: "Point" },
      id: "sm6ef753fb",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.367600115, 37.001491996], type: "Point" },
      id: "sm20de3f1b",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.364697966, 37.002854337], type: "Point" },
      id: "smbbe4046c",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363914762, 37.003231334], type: "Point" },
      id: "sm93f01508",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.362452959, 37.003882504], type: "Point" },
      id: "sm9d91a463",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361176228, 37.004475835], type: "Point" },
      id: "sm2d4970f1",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360500311, 37.004912795], type: "Point" },
      id: "sm45a8ac64",
      properties: {},
      type: "Feature",
    },
  ],
  id: "root",
  type: "FeatureCollection",
};
const GRAND_FULL_COURSE_GEO_JSON = {
  features: [
    {
      geometry: { coordinates: [-86.363608991, 37.003595476], type: "Point" },
      id: "sma85350e6",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363525842, 37.004265921], type: "Point" },
      id: "sm7270301a",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363013541, 37.004657904], type: "Point" },
      id: "sm0ccceba4",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.36270777, 37.004837829], type: "Point" },
      id: "sm0c2176b8",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.362694359, 37.005386171], type: "Point" },
      id: "sme0fd315c",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.362514652, 37.005653915], type: "Point" },
      id: "sm02710934",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.36207477, 37.00575887], type: "Point" },
      id: "sm3d2e60bd",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361728765, 37.005591798], type: "Point" },
      id: "sm8ca40e45",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361498095, 37.005178401], type: "Point" },
      id: "sm7eabddd0",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361648299, 37.004884951], type: "Point" },
      id: "sm20b9ada9",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361849464, 37.00446084], type: "Point" },
      id: "sm8dc776ea",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361669756, 37.004139542], type: "Point" },
      id: "smf243ba54",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361197687, 37.0040303], type: "Point" },
      id: "sm8ec26ec5",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360677338, 37.004278771], type: "Point" },
      id: "sm271a0c2e",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360570052, 37.004632199], type: "Point" },
      id: "sm51becaf8",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360500311, 37.004912795], type: "Point" },
      id: "sm45a8ac64",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360390341, 37.005356181], type: "Point" },
      id: "sm04eb3d03",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360344743, 37.005645345], type: "Point" },
      id: "sm465d5bbd",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.36043862, 37.005945216], type: "Point" },
      id: "sm42d287ef",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.360819493, 37.006125139], type: "Point" },
      id: "sma0ecb2c4",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.361404215, 37.006365035], type: "Point" },
      id: "sm74d18684",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.362300073, 37.006446429], type: "Point" },
      id: "sm52a30982",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.362911616, 37.006262223], type: "Point" },
      id: "smb17c50e8",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363415872, 37.00589381], type: "Point" },
      id: "smf0bb72ef",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363786017, 37.005251225], type: "Point" },
      id: "sm9ac9ffed",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.364220534, 37.00460435], type: "Point" },
      id: "smc9c2ad47",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.364719425, 37.004171671], type: "Point" },
      id: "sm94cca9a9",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.365277324, 37.003691863], type: "Point" },
      id: "sm641fc855",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.365845952, 37.003130656], type: "Point" },
      id: "sm046db624",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.366473589, 37.002629422], type: "Point" },
      id: "sm7a3043c1",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368034635, 37.001926832], type: "Point" },
      id: "sm54c39eea",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368689093, 37.001665502], type: "Point" },
      id: "sm6991b5aa",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369606408, 37.001691207], type: "Point" },
      id: "sm7ad11996",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370078477, 37.001695491], type: "Point" },
      id: "smedf14920",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370443257, 37.001348477], type: "Point" },
      id: "sm67431388",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370743665, 37.001091428], type: "Point" },
      id: "smaf2eee8b",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371446404, 37.000778684], type: "Point" },
      id: "sm9a5158f7",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371945295, 37.000555907], type: "Point" },
      id: "smb584eea4",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372192058, 37.000598749], type: "Point" },
      id: "sm427af187",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372256431, 37.000855799], type: "Point" },
      id: "smcd26cfbd",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372127685, 37.001151406], type: "Point" },
      id: "sma7945d34",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371811184, 37.001545547], type: "Point" },
      id: "sm1c38f1d4",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371280107, 37.001832583], type: "Point" },
      id: "sm4ea18f36",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370362791, 37.00211105], type: "Point" },
      id: "smdb30d270",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369971189, 37.002380947], type: "Point" },
      id: "sm9c482eac",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369702968, 37.002903603], type: "Point" },
      id: "sm2d668b8e",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369381103, 37.003353427], type: "Point" },
      id: "sm43ee265f",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368785652, 37.00345196], type: "Point" },
      id: "sm7eeb1071",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368383321, 37.003254895], type: "Point" },
      id: "sm0fe86ac3",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.367879066, 37.002916455], type: "Point" },
      id: "sme40c2b8d",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.367546472, 37.002903603], type: "Point" },
      id: "sm873274a6",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.366940292, 37.00316493], type: "Point" },
      id: "sme62aa6c3",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.36650041, 37.00366616], type: "Point" },
      id: "sm8ec06207",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.366505775, 37.003974608], type: "Point" },
      id: "smaf8b045c",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.367691311, 37.004313043], type: "Point" },
      id: "smb5843186",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.368447694, 37.004467266], type: "Point" },
      id: "sma2e94d94",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369000229, 37.004394438], type: "Point" },
      id: "smb91bcaf6",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369456204, 37.004175956], type: "Point" },
      id: "sm7f70cc04",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.369922909, 37.003691865], type: "Point" },
      id: "smfc42c285",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.370829495, 37.002663696], type: "Point" },
      id: "sma6c5c962",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372068676, 37.001948254], type: "Point" },
      id: "sm2f1dfe11",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372841153, 37.001271363], type: "Point" },
      id: "smfd15f9d6",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.374214444, 36.999823309], type: "Point" },
      id: "smb46027b1",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.37438074, 36.999564113], type: "Point" },
      id: "sm0e9e66c5",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.374321731, 36.999242794], type: "Point" },
      id: "sm83364f72",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.374192985, 36.999009302], type: "Point" },
      id: "sm09cb4e0c",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.373747739, 36.998990023], type: "Point" },
      id: "sme1d98c0c",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.37298063, 36.999154968], type: "Point" },
      id: "sm496a41a7",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.372288617, 36.999343474], type: "Point" },
      id: "sm197bb154",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.371164771, 36.999844729], type: "Point" },
      id: "smbdcde48f",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.36983976, 37.000433808], type: "Point" },
      id: "sm6ef753fb",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.367600115, 37.001491996], type: "Point" },
      id: "sm20de3f1b",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.364697966, 37.002854337], type: "Point" },
      id: "smbbe4046c",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363914762, 37.003231334], type: "Point" },
      id: "sm93f01508",
      properties: {},
      type: "Feature",
    },
    {
      geometry: { coordinates: [-86.363608991, 37.003595476], type: "Point" },
      id: "sma85350e6",
      properties: {},
      type: "Feature",
    },
  ],
  id: "root",
  type: "FeatureCollection",
};
const CORVETTE_RACE_LOOP = CORVETTE_RACE_LOOP_GEO_JSON.features.map(
  (feature) => feature.geometry.coordinates,
);
const GRAND_MAX_STRAIGHT = GRAND_MAX_STRAIGHT_GEO_JSON.features.map(
  (feature) => feature.geometry.coordinates,
);
const GRAND_FULL_COURSE = GRAND_FULL_COURSE_GEO_JSON.features.map(
  (feature) => feature.geometry.coordinates,
);
const raceTrackGeoJSON = {
  features: [
    {
      geometry: {
        coordinates: RANDOM_TRACK,
        type: "LineString",
      },
      properties: {},
      type: "Feature",
    },
  ],
  type: "FeatureCollection",
} as const satisfies FeatureCollection<LineString>;
const raceTrackGeoJSON2 = {
  features: [
    {
      geometry: {
        coordinates: RANDOM_TRACK.map((coord) =>
          coord.map((miniCoord) => miniCoord + 0.5),
        ),
        type: "LineString",
      },
      properties: {},
      type: "Feature",
    },
  ],
  type: "FeatureCollection",
} as const satisfies FeatureCollection<LineString>;
const raceTrackGeoJSON_CORVETTE_RACE_LOOP = {
  features: [
    {
      geometry: {
        coordinates: CORVETTE_RACE_LOOP,
        type: "LineString",
      },
      properties: {},
      type: "Feature",
    },
  ],
  type: "FeatureCollection",
} as const satisfies FeatureCollection<LineString>;
const raceTrackGeoJSON_GRAND_MAX_STRAIGHT = {
  features: [
    {
      geometry: {
        coordinates: GRAND_MAX_STRAIGHT,
        type: "LineString",
      },
      properties: {},
      type: "Feature",
    },
  ],
  type: "FeatureCollection",
} as const satisfies FeatureCollection<LineString>;
const raceTrackGeoJSON_GRAND_FULL_COURSE = {
  features: [
    {
      geometry: {
        coordinates: GRAND_FULL_COURSE,
        type: "LineString",
      },
      properties: {},
      type: "Feature",
    },
  ],
  type: "FeatureCollection",
} as const satisfies FeatureCollection<LineString>;

const trackLayerStyle: LayerProps = {
  layout: {
    "line-cap": "round",
    "line-join": "round",
  },
  paint: {
    "line-color": "#ff0000", // Red color for the track
    "line-width": 4, // Thickness of the track
  },
  type: "line",
};

/**
 * Calculates the linear interpolation (lerp) between two numbers.
 * @param startPosition - The starting value.
 * @param endPosition - The ending value.
 * @param timeOfAnimation - The ratio (between 0 and 1) representing the progress between start and end.
 * @returns The interpolated value between startPosition and endPosition.
 */
const lerp = (
  startPosition: number,
  endPosition: number,
  timeOfAnimation: number,
) => {
  return startPosition * (1 - timeOfAnimation) + endPosition * timeOfAnimation;
};

const fitBounds = (
  mapRef: MapRef | undefined,
  coordsA: Coords,
  coordsB: Coords,
) => {
  if (!mapRef) return;
  mapRef.fitBounds(
    [
      [coordsA.long, coordsA.lat],
      [coordsB.long, coordsB.lat],
    ],
    {
      linear: true,
      maxZoom: 16,
      padding: { bottom: 35, left: 35, right: 35, top: 35 },
    },
  );
};
const isOutsideBounds = (
  mapRef: MapRef | undefined,
  coordinates: Coords[],
): boolean => {
  if (!mapRef || !coordinates) return false;
  const bounds = mapRef.getBounds();
  if (!bounds) return false;
  const { lat: northLat, lng: eastLng } = bounds.getNorthEast();
  const { lat: southLat, lng: westLng } = bounds.getSouthWest();
  coordinates.forEach((coord) => {
    if (
      coord.long < westLng ||
      coord.long > eastLng ||
      coord.lat < southLat ||
      coord.lat > northLat
    ) {
      return true;
    }
  });
  return false;
};
export const GEO_DATA = {
  raceTrackGeoJSON,
  raceTrackGeoJSON2,
  raceTrackGeoJSON_CORVETTE_RACE_LOOP,
  raceTrackGeoJSON_GRAND_FULL_COURSE,
  raceTrackGeoJSON_GRAND_MAX_STRAIGHT,
};
export const TRACK_LIST: TrackList[] = [
  {
    layerProps: {
      ...trackLayerStyle,
      paint: { ...trackLayerStyle.paint, "line-color": "#ff0000" },
    },
    sourceProps: {
      data: raceTrackGeoJSON_CORVETTE_RACE_LOOP,
      id: "layer1",
      type: "geojson",
    },
    trackName: "Corvette Race Loop",
  },
  {
    layerProps: {
      ...trackLayerStyle,
      paint: { ...trackLayerStyle.paint, "line-color": "#0f00ff" },
    },
    sourceProps: {
      data: raceTrackGeoJSON_GRAND_FULL_COURSE,
      id: "layer2",
      type: "geojson",
    },
    trackName: "Grand Full Course",
  },
  {
    layerProps: {
      ...trackLayerStyle,
      paint: { ...trackLayerStyle.paint, "line-color": "#ff00ff" },
    },
    sourceProps: {
      data: raceTrackGeoJSON_GRAND_MAX_STRAIGHT,
      id: "layer3",
      type: "geojson",
    },
    trackName: "Grand Max Straight",
  },
] as const;

const distance = (x1: number, y1: number, x2: number, y2: number): number => {
  return Math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2);
};
export const mapCameraControls = {
  distance,
  fitBounds,
  isOutsideBounds,
  lerp,
};

export const Hydrated_Grand_Full_course: PacketMarkerData[] =
  raceTrackGeoJSON_GRAND_FULL_COURSE.features[0].geometry.coordinates.map(
    (coords) => {
      const newPacketMarker: PacketMarkerData = {
        data: generateFakeTelemetryData(),
        markerCoords: {
          latitude: coords[1]!,
          longitude: coords[0]!,
        },
        open: false,
      };
      return newPacketMarker;
    },
  );