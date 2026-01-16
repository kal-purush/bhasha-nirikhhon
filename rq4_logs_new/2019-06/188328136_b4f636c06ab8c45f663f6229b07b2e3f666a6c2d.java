package com.macoredroid.onlinebookstore.serviceimpl;

import com.macoredroid.onlinebookstore.dao.BooklistDao;
import com.macoredroid.onlinebookstore.entity.Booklist;
import com.macoredroid.onlinebookstore.service.ChangeBookService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ChangeBookServiceimpl implements ChangeBookService {
    @Autowired
    private BooklistDao BooklistDao;
    @Override
    public boolean changeName(String bookID, String newbookname) {
        Booklist tempBook= BooklistDao.findByName(newbookname);
        if(tempBook!=null)
        {
            return false;
        }
        else
        {
            Booklist oldBook= BooklistDao.findOne(Integer.parseInt(bookID));
            if(oldBook==null)
            {
                return false;
            }
            else
            {
                oldBook.setName(newbookname);
                BooklistDao.save(oldBook);
                return true;
            }
        }

    }
}