package project.TadeM.product.service;

import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import project.TadeM.product.entity.product;
import project.TadeM.product.model.productInput;
import project.TadeM.product.repository.productRepository;

@RequiredArgsConstructor
@Service
public class productServiceImpl implements productService{

	private final productRepository productRepository;

	@Override
	public boolean write(productInput parameter) {

		product pr = product.builder()
			.name(parameter.getName())
			.regDt(LocalDateTime.now())
			.build();

		productRepository.save(pr);

		return true;
	}
}