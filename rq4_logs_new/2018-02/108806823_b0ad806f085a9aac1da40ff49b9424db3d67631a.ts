import * as shelljs from 'shelljs';
import chalk from 'chalk';

console.log(chalk.cyan('react-native-camera ! workaround for Non-FaceDetection\n'));

shelljs.rm('-rf', 'node_modules/react-native-camera/ios/FaceDetector');
shelljs.cp(
  'node_modules/react-native-camera/postinstall_project/projectWithoutFaceDetection.pbxproj',
  'node_modules/react-native-camera/ios/RNCamera.xcodeproj/project.pbxproj',
);