package gigjob.controller;

import gigjob.dto.WalletDTO;
import gigjob.entity.Account;
import gigjob.entity.ResponseObject;
import gigjob.entity.Wallet;
import gigjob.repository.AccountRepository;
import gigjob.repository.WalletRepository;
import gigjob.util.WalletMapper;
import lombok.extern.log4j.Log4j2;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Log4j2
@RequestMapping(value = "/api/v1/wallet")
public class WalletController {
    @Autowired
    private ModelMapper modelMapper;
    @Autowired
    private WalletRepository walletRepository;
    @Autowired
    private AccountRepository accountRepository;

    @GetMapping
    public ResponseEntity<ResponseObject> getAll() {
        List<WalletDTO> walletDTOS = walletRepository.findAll()
                .stream()
                .map(WalletMapper::toDto)
                .toList();
        ResponseObject responseObject = new ResponseObject(HttpStatus.OK.toString(), "Get all successfully", walletDTOS);
        return ResponseEntity.status(HttpStatus.OK).body(responseObject);
    }

    @PostMapping
    public ResponseEntity<ResponseObject> create(@RequestBody WalletDTO walletDTO) {
        Wallet wallet = modelMapper.map(walletDTO, Wallet.class);
        Account account = accountRepository.findById(walletDTO.getAccountId()).orElse(null);
        ResponseObject responseObject;
        HttpStatus status = HttpStatus.OK;
        if (account != null) {
            wallet.setAccount(account);
            walletRepository.save(wallet);
            responseObject = new ResponseObject(HttpStatus.OK.toString(), "Get all successfully", walletDTO);
        } else {
            status = HttpStatus.NOT_FOUND;
            responseObject = new ResponseObject(HttpStatus.NOT_FOUND.toString(), "Not found account", null);
        }
        return new ResponseEntity<ResponseObject>(responseObject, status);
    }
}