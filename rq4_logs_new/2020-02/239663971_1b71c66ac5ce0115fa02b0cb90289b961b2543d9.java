package pe.edu.ceid.simi.management.application.voucher;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import pe.edu.ceid.simi.management.domain.voucher.model.Voucher;
import pe.edu.ceid.simi.management.domain.voucher.model.VoucherDTO;
import pe.edu.ceid.simi.management.domain.voucher.repository.VoucherRepository;

@Service
public class VoucherServiceImpl implements VoucherService{

	@Autowired
	private VoucherRepository repository;
	
	@Override
	public List<VoucherDTO> getVouchers() {
		return this.repository.getVouchers();
	}

	@Override
	public boolean saveVouchers(List<Voucher> vouchers) {
		return this.repository.saveVouchers(vouchers);
	}
}