import Operator from './models/Operator';

const OPERATORS = {
  MORE_THAN_EQUAL: new Operator(true, true, false),
  MORE_THAN: new Operator(true, false, false),
  EQUAL: new Operator(false, true, false),
  LESS_THAN_EQUAL: new Operator(false, true, true),
  LESS_THAN: new Operator(false, false, true),
};

function isVersionValid(
  currentVersion: string, operator: Operator, checkVersion: string,
): boolean {
  if (checkVersion === '*') {
    // meaning all versions
    return true;
  }

  // assumption 999 is the biggest value for each value
  // hence in binary form will not be more than 10 positions
  const [major, minor, patch] = currentVersion.split('.');

  const majorNum = parseInt(major.replace('v', ''), 10);
  const minorNum = parseInt(minor, 10);
  const patchNum = parseInt(patch, 10);

  const currCheckSum = (majorNum << 20) | (minorNum << 10) | (patchNum);

  const [checkMajor, checkMinor, checkPatch] = checkVersion.split('.');

  const checkMajorNum = parseInt(checkMajor.replace('v', ''), 10);
  const checkMinorNum = parseInt(checkMinor, 10);
  const checkPatchNum = parseInt(checkPatch, 10);

  const checkSum = (checkMajorNum << 20) | (checkMinorNum << 10) | checkPatchNum;

  if (operator.isMore) {
    return (
      currCheckSum > checkSum)
      || (operator.isEqual && currCheckSum >= checkSum
      );
  } if (operator.isLess) {
    return (
      currCheckSum < checkSum
      || (operator.isEqual && currCheckSum <= checkSum)
    );
  }

  // meaning equals
  return (currCheckSum === checkSum);
}

export default {
  OPERATORS,
  isVersionValid,
};