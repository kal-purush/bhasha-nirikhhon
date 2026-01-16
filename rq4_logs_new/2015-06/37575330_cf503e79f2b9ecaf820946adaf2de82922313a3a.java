package misskey.com.misskey.com.server;

import android.content.Context;
import android.content.Intent;

import org.apache.http.HttpStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import misskey.com.db.ThreadDAO;
import misskey.com.db.ThreadDAoImp;
import misskey.com.entities.FileInfo;
import misskey.com.entities.ThreadInfo;

/**  下载任务类
 * Created by misskey on 15-6-17.
 */
public class DownloadTask {
    private Context mContext=null;
    private FileInfo mFileInfo=null;
    private ThreadDAO mDao=null;
    protected boolean isPaused=false;
    public DownloadTask(Context mContext, FileInfo mFileInfo) {
        this.mContext = mContext;
        this.mFileInfo = mFileInfo;
        mDao=new ThreadDAoImp(mContext);
    }

    public  void download(){
        //读取数据库的线程信息
      List<ThreadInfo> threadInfos= mDao.getThread(mFileInfo.getUrl());
        ThreadInfo threadInfo=null;
        if(threadInfos.size()==0){
            //初始化线程信息对象
            threadInfo=new ThreadInfo(0,mFileInfo.getUrl(),0,mFileInfo.getLength(),0);
        }else {
            threadInfo =threadInfos.get(0);

        }//创建子线程下载
        new DownloadThread(threadInfo).start();
    }
    /**
     * 现在线程
     */
    class DownloadThread extends  Thread{
        private ThreadInfo mThreadInfo=null;
        private  int mFinished;
        public DownloadThread(ThreadInfo mThreadInfo){
            this.mThreadInfo=mThreadInfo;
        }

        @Override
        public void run() {
            HttpURLConnection conn=null;
            RandomAccessFile raf=null;
            InputStream input=null;
            //想数据库插入线程信息
            if(!mDao.isExits(mThreadInfo.getUrl(),mThreadInfo.getId())){
                mDao.insertThread(mThreadInfo);
            }

            try {
                URL url=new URL(mThreadInfo.getUrl());
                conn = (HttpURLConnection) url.openConnection();
                conn.setConnectTimeout(3000);
                conn.setRequestMethod("GET");
                //设置下载位置
                int start=mThreadInfo.getStart()+mThreadInfo.getIsFinish();
                //多线程下载的重点
                conn.setRequestProperty("Range","bytes="+start+"-"+mThreadInfo.getEnd());
                File file=new File(DownLoadService.DOWNLOAD_PATH,mFileInfo.getFileName());
                raf=new RandomAccessFile(file,"rwd");
                //文件写入位置
                raf.seek(start);
                Intent intent=new Intent(DownLoadService.ACTION_UPDATE);
                mFinished+=mThreadInfo.getIsFinish();
                //开始下载
                if(conn.getResponseCode()== HttpStatus.SC_PARTIAL_CONTENT){
                    //读取数据
                    input=conn.getInputStream();
                    byte[] buffer=new byte[1024*4];
                    int len=-1;
                    long time=System.currentTimeMillis();
                    while((len=input.read(buffer))!=-1){
                         //写入文件
                        raf.write(buffer,0,len);
                        mFinished+=len;
                        if(System.currentTimeMillis()-time>500){
                            time=System.currentTimeMillis();
                            intent.putExtra("fininshed",mFinished*100/mFileInfo.getLength());
                            mContext.sendBroadcast(intent);
                        }
                        //下载暂停是,保存下载进度
                        if(isPaused){
                            mDao.updateThreadInfo(mThreadInfo.getUrl(),
                                    mThreadInfo.getId(),mFinished);
                            return;
                        }
                    }
                    mDao.deleteThreadInfo(mThreadInfo.getUrl(),mThreadInfo.getId());

                }

            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                 conn.disconnect();
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    raf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}