module.exports = (length) => {
  console.log(process.stdout.columns);
  const end = process.stdout.columns - 2;

  return (current) => {
    let percentage = current / length * 100;
    if (percentage > 100) {
      percentage = 100;
    }

    let finishedCount = Math.floor(percentage + end / 100);
    if (finishedCount > end) {
      finishedCount = end;
    }

    let remaining = end - finishedCount;
    if (remaining < 0) {
      remaining = 0;
    }

    const log = `[${'#'.repeat(finishedCount)}${'-'.repeat(remaining)}]`;

    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(log);
  };
};