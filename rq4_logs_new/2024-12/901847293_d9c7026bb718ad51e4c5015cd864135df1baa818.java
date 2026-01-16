package com.tinqin.library.reporting.domain.clients;


import org.springframework.cloud.openfeign.FeignClient;
import com.tinqin.library.restexport.BookRestExport;

@FeignClient(name = "bookClient",url = "${book.url}",
        configuration = BookClientConfiguration.class)
public interface BookClient extends BookRestExport {

}