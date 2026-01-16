import * as XLSX from 'xlsx';

export const readExcelFile = async (file: string[]) => {
  // const fileReader = new FileReader();
  // const readAsArrayBuffer = fileReader.readAsArrayBuffer(file[0] as any);

  // fileReader.onload = (e) => {
  //   const bufferArray = e.target?.result;

  //   const workBook = XLSX.read(bufferArray, { type: 'buffer' });
  //   const workSheetName = workBook.SheetNames[0];
  //   const workSheet = workBook.Sheets[workSheetName];

  //   console.log(readAsArrayBuffer, workBook);

  //   const data = XLSX.utils.sheet_to_json(workSheet);
  //   console.log('data', data);

  //   return data;
  // };

  const promise = await new Promise((resolve, reject) => {
    const fileReader = new FileReader();
    if (file[0]) {
    }
    fileReader.readAsArrayBuffer(file[0] as any);

    fileReader.onload = async (e) => {
      const bufferArray = await e.target?.result;

      const readBuffer = await XLSX.read(bufferArray, { type: 'buffer' });

      const getSheetName = await readBuffer.SheetNames[0];

      const getSheet = await readBuffer.Sheets[getSheetName];

      const getData = await XLSX.utils.sheet_to_json(getSheet);

      console.log(
        'data',
        getData.map((data: any) => {
          return { contact_number: data.contact_number };
        })
      );

      console.log('resolve(getData)', getData);

      return getData;
    };

    fileReader.onerror = (error) => {
      console.log('Error on import the excel file', error);
    };

    // let res = await promise.then(result => {

    //   setRecipients(result as string[])
    // })
  });

  return promise;
};