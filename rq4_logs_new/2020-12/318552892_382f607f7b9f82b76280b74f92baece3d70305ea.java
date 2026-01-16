package com.zhang.libweb.service.reback.impl;

import com.zhang.libweb.constants.ResultDTO;
import com.zhang.libweb.enums.HttpCode;
import com.zhang.libweb.enums.ValidFlagEnum;
import com.zhang.libweb.mapper.book.BookMapper;
import com.zhang.libweb.mapper.borrow.BookBorrowMapper;
import com.zhang.libweb.model.book.BookDTO;
import com.zhang.libweb.model.borrow.BookBorrowDTO;
import com.zhang.libweb.service.reback.RebackService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class RebackServiceImpl implements RebackService {

    @Autowired
    private BookMapper bookMapper;

    @Autowired
    private BookBorrowMapper bookBorrowMapper;

    @Override
    public ResultDTO updateBorrowInfo(BookBorrowDTO bookBorrowDTO) {
        int boodId = bookBorrowDTO.getBookId();
        if (0 >= boodId) {
            return new ResultDTO(HttpCode.FAIL.getCode(), "书籍ID不能为空");
        }
        if(0>=bookBorrowDTO.getId()){
            return new ResultDTO(HttpCode.FAIL.getCode(),"数据主键不能为空");
        }
        //查找本地是否存在这个书籍
        BookDTO bookDTO = bookMapper.selectByPrimaryKey(boodId);
        if (null == bookDTO) {
            return new ResultDTO(HttpCode.FAIL.getCode(), "书籍信息不存在");
        }

        //本地借书信息查询
        BookBorrowDTO localBookBorrowDTO = bookBorrowMapper.selectByPrimaryKey(bookBorrowDTO.getId());
        if (null == localBookBorrowDTO) {
            return new ResultDTO(HttpCode.SUCCESS.getCode(), "借书信息不存在，请确保信息合法或联系系统管理员");
        }
        //2.1-本地借书得数量
        int borrowCount = localBookBorrowDTO.getCount();
        int tmp1Count = 0;
        if (!StringUtils.isEmpty(localBookBorrowDTO.getTmp1())) {
            tmp1Count = Integer.parseInt(localBookBorrowDTO.getTmp1());
        }

        if (tmp1Count == borrowCount) {
            return new ResultDTO(HttpCode.SUCCESS.getCode(), "您已经还完全部书籍，无需归还");
        }
        //2.2-还书的数量
        int rebackCount = bookBorrowDTO.getCount();

        return doBack(rebackCount, borrowCount, localBookBorrowDTO, bookDTO,tmp1Count);

    }

    /**
     * 真正的还书操作
     * @param rebackCount          还书数量
     * @param borrowCount           借书数量
     * @param localBookBorrowDTO    本地借书实体类
     * @param bookDTO               本地书籍实体类
     * @return
     */
    private ResultDTO doBack(int rebackCount, int borrowCount, BookBorrowDTO localBookBorrowDTO, BookDTO bookDTO,int tmp1Count) {

        int nowTmpCount = tmp1Count;
        //3-还书判断
        if (rebackCount<0){
            return new ResultDTO(HttpCode.FAIL.getCode(), "归还数量小于0，请确保归还数量正确");
        }
        if (rebackCount+nowTmpCount > borrowCount) {
            return new ResultDTO(HttpCode.FAIL.getCode(), "归还数量大于待还数量，请确保归还数量正确");
        } else if (rebackCount+nowTmpCount == borrowCount) {
            //借书还书一样，数据DISABLE,book表数量增加
            //1.操作还书信息表
            localBookBorrowDTO.setCount(0);
            //本地上一次归还的数量
//            if (!StringUtils.isEmpty(localBookBorrowDTO.getTmp1())) {
//                nowTmpCount = Integer.parseInt(localBookBorrowDTO.getTmp1());
//            }

            localBookBorrowDTO.setTmp1(String.valueOf(rebackCount + nowTmpCount));
            localBookBorrowDTO.setTmp2(String.valueOf(new Date()));
            localBookBorrowDTO.setValidFlagEnum(String.valueOf(ValidFlagEnum.DISABLE));
            bookBorrowMapper.updateByPrimaryKeySelective(localBookBorrowDTO);
            //2.追加book信息表剩余数量
            bookDTO.setBookCount(bookDTO.getBookCount() + rebackCount);
            bookDTO.setUpdateDate(new Date());
            bookMapper.updateByPrimaryKeySelective(bookDTO);
            return new ResultDTO(HttpCode.SUCCESS.getCode(), "归还成功，书籍已经全部归还");

        } else {
            //1.还书数量小于借书数量，更新还书数量(temp1),同时更新book剩余数量
//            int nowTmpCount = 0;
//            if (!StringUtils.isEmpty(localBookBorrowDTO.getTmp1())) {
//                nowTmpCount = Integer.parseInt(localBookBorrowDTO.getTmp1());
//            }
            localBookBorrowDTO.setTmp1(String.valueOf(rebackCount + nowTmpCount));
            localBookBorrowDTO.setTmp2(String.valueOf(new Date()));
            //没还完不需要修改validFlag
            //localBookBorrowDTO.setValidFlagEnum(String.valueOf(ValidFlagEnum.DISABLE));
            //2.追加book信息表剩余数量
            bookDTO.setBookCount(bookDTO.getBookCount() + rebackCount);
            bookDTO.setUpdateDate(new Date());

            int finalBackCount = (borrowCount - (rebackCount+nowTmpCount));
            if(finalBackCount<0){
                return new ResultDTO(HttpCode.FAIL.getCode(), "归还数量大于借阅数量，请确保归还数量正确");
            }else if(finalBackCount == 0 ){
                localBookBorrowDTO.setCount(0);
                localBookBorrowDTO.setValidFlagEnum(String.valueOf(ValidFlagEnum.DISABLE));
                bookBorrowMapper.updateByPrimaryKeySelective(localBookBorrowDTO);
                bookMapper.updateByPrimaryKeySelective(bookDTO);
                return new ResultDTO(HttpCode.FAIL.getCode(), "归还成功，书籍已经全部归还");
            }else{
                localBookBorrowDTO.setCount(finalBackCount);
                bookBorrowMapper.updateByPrimaryKeySelective(localBookBorrowDTO);
                bookMapper.updateByPrimaryKeySelective(bookDTO);
                return new ResultDTO(HttpCode.SUCCESS.getCode(), "归还成功,还有" + (borrowCount
                        - (rebackCount+nowTmpCount)) + "本书尚未归还，请在" + localBookBorrowDTO.getEndDate() + "之前归还剩余书籍");
            }
        }
    }
}