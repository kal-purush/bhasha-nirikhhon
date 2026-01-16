import { useAtom } from 'jotai';
import { useEffect } from 'react';
import { ENV } from '../Constants.ts';
import { FileInfo } from '../types.ts';
import { filelistAtom } from './atoms.ts';

const _fetchFunction = async () => {
  const filelistUrl = ENV.fileList;
  if (!filelistUrl) {
    alert('.env에 환경변수를 설정해주세요');
    return;
  }
  return fetch(filelistUrl, { cache: 'no-store' })
    .then(res => res.json())
    .then((filelist: FileInfo[]) =>
    // 최신순으로 정렬
    {
      const models = filelist.filter(
        fileinfo =>
          fileinfo.filename.endsWith('.gltf') ||
          fileinfo.filename.endsWith('.glb'),
      );
      const envs = filelist.filter(
        fileinfo =>
          fileinfo.filename.endsWith('.hdr') ||
          fileinfo.filename.endsWith('.exr'),
      );

      const scenes = filelist.filter(fileinfo =>
        fileinfo.filename.endsWith('.json'),
      );

      models.reverse();
      envs.reverse();
      const retval = {
        models,
        envs,
        scenes,
        all: filelist,
      };
      return retval;
    },
    )
    .catch(e => {
      console.error(e);
      alert('파일리스트를 불러오는데 실패했습니다. (콘솔확인)');
    });
};

export default function useFilelist() {
  const [filelist, setFilelist] = useAtom(filelistAtom);
  const fetchFunction = () =>
    _fetchFunction()
      .then(list => {
        if (list) {
          setFilelist(list);
        }
      })
      .catch(e => {
        console.error(e);
        alert('파일리스트를 불러오는데 실패했습니다. : ' + e.message);
      });
  useEffect(() => {
    fetchFunction();
  }, []);
  const loading = filelist === null;
  const refetch = fetchFunction;
  return { filelist, loading, refetch };
}