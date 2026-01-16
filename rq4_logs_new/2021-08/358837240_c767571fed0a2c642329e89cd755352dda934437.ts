import { readdir, writeFile } from 'fs/promises';
import jsonData from '../data/kabupaten/34.json';
import schoolData from '../data/school.json';
import path from 'path';

const getListOfFiles = (): Promise<string[]> => {
  const dir = path.join(__dirname, '../data/kabupaten');
  return readdir(dir);
};

const getNewId = (oldString: string, newId: string) => {
  const remainingString = oldString.substring(2);
  return [newId, remainingString].join('');
};

const changeProvinceCode = async () => {
  const files = await getListOfFiles();
  files.forEach(async (file) => {
    const filePath = path.join(__dirname, '../data/kabupaten/', file);
    const newId = path.basename(filePath, '.json');
    const { default: citiesData } = await import(filePath);
    const newData = citiesData.map((data: { id: string; nama: string }) => {
      data.id = getNewId(data.id, newId);
      return data;
    });
    writeFile(filePath, JSON.stringify(newData), {
      encoding: 'utf-8',
    });
  });
};

const changeKabupatenCode = async () => {
  const newData = jsonData.map((data) => {
    const filteredSchoolData = schoolData.dataSekolah.find(
      (school) => school.kabupaten_kota.toLowerCase() === data.nama.toLowerCase(),
    );
    data.id = filteredSchoolData?.kode_kab_kota ?? '0';
    return data;
  });
  await writeFile(path.join(__dirname, '../data/kabupaten/34.json'), JSON.stringify(newData), {
    encoding: 'utf-8',
  });
};

changeProvinceCode();
changeKabupatenCode();