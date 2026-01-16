




// curl -X POST \
// http://localhost:3060/notification/log/create \
// -H 'Accept: */*' \
// -H 'Accept-Encoding: gzip, deflate' \
// -H 'Cache-Control: no-cache' \
// -H 'Connection: keep-alive' \
// -H 'Content-Length: 236' \
// -H 'Content-Type: application/json' \
// -H 'Host: localhost:3060' \
// -H 'Postman-Token: 42c353ff-3fcb-4bb9-a773-dbe89b1dac2a,902fd5e6-7259-4eb4-a26b-5bd6fd715a5e' \
// -H 'User-Agent: PostmanRuntime/7.20.1' \
// -H 'cache-control: no-cache' \
// -d '{
//   "notifications": [
//       {
//           "notification_id": "10001N",
//           "notification_click": true,
//           "notification_view": false,
//           "title": "Notification Service",
//           "message": "Message Delevered",
//           "subscriber": {
//               "subscriber_id": "1001SUB",
//               "site_id": "101SIT"
//           }
//       },
//       {
//           "notification_id": "10002N",
//           "notification_click": true,
//           "notification_view": false,
//           "title": "Notification Service",
//           "message": "Message Delevered",
//           "subscriber": {
//               "subscriber_id": "1002SUB",
//               "site_id": "102SIT"
//           }
//       }
//   ]
// }'

//Response

// {
//     "status": true,
//     "message": "Request Processed successfully",
//     "response": {}
// }





/* ------------------------------------------------------*/
// 2: Analytics details api

// curl -X POST \
//   http://localhost:3060/notification/analytics/details \
//   -H 'Accept: */*' \
//   -H 'Accept-Encoding: gzip, deflate' \
//   -H 'Cache-Control: no-cache' \
//   -H 'Connection: keep-alive' \
//   -H 'Content-Length: 27' \
//   -H 'Content-Type: application/json' \
//   -H 'Host: localhost:3060' \
//   -H 'Postman-Token: 8b032bba-2690-41f4-ab8d-e42e3a4f8f41,83c08f6c-cea3-4b62-9bff-63d86a906789' \
//   -H 'User-Agent: PostmanRuntime/7.20.1' \
//   -H 'cache-control: no-cache' \
//   -d '{
//     "site_id": "101SIT"
// }'

//Response:

// {
//     "status": true,
//     "message": "Request Processed successfully",
//     "response": {
//         "notification_view_records": [
//             {
//                 "_id": true,
//                 "notification_click": 11
//             }
//         ],
//         "notification_click_records": [
//             {
//                 "_id": true,
//                 "notification_view": 9
//             }
//         ],
//         "site_records": [
//             {
//                 "subscriber_Details": [
//                     {
//                         "_id": "5df6f09c5d73092ace9d2b69",
//                         "notification_id": "10001N",
//                         "notification_click": true,
//                         "notification_view": false,
//                         "title": "Notification Service",
//                         "subscriber": {
//                             "subscriber_id": "1001SUB",
//                             "site_id": "101SIT"
//                         },
//                         "epoch": 1576464540870,
//                         "__v": 0
//                     }
//                 ],
//                 "groupByDay": [
//                     {
//                         "_id": true,
//                         "notification_view": 9
//                     }
//                 ],
//                 "groupByWeek": [
//                     {
//                         "_id": true,
//                         "notification_view": 9
//                     }
//                 ],
//                 "groupByMonth": [
//                     {
//                         "_id": true,
//                         "notification_view": 9
//                     }
//                 ]
//             }
//         ]
//     }
// }