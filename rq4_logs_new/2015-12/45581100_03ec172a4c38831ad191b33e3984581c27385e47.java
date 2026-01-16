package incircle.api;

import incircle.account.dao.AccountDao;
import incircle.account.model.Account;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

/**
 * Created by Julian on 12/12/2015.
 */
@Controller
public class UploadFileController {
    private AccountDao accountDao;
    @Autowired
    public void setAccountDao(AccountDao accountDao) {
        this.accountDao = accountDao;
    }

    @RequestMapping(value = "/uploadImage", method = RequestMethod.POST)
    public @ResponseBody void uploadImage(@RequestParam("file") MultipartFile file) {
        if (!file.isEmpty()) {
            Account account = new Account("wocao", "wocao", true);
            try {
                accountDao.uploadImage(file, account);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @RequestMapping(value = "/uploadVideo", method = RequestMethod.POST)
    public @ResponseBody void uploadVideo(@RequestParam("file") MultipartFile file) {
        if (!file.isEmpty()) {
            Account account = new Account("wocao", "wocao", true);
            try {
                accountDao.uploadVideo(file, account);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}