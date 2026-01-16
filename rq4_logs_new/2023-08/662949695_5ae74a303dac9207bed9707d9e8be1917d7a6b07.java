package edu.zuel.hahasearch.dao;

import edu.zuel.hahasearch.model.domain.ESResult;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Highlight;
import org.springframework.data.elasticsearch.annotations.HighlightField;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ESResultRepository extends ElasticsearchRepository<ESResult,String> {
    @Highlight(fields = {
            @HighlightField(name = "title"),
            @HighlightField(name = "content")
    })
    Page<ESResult> searchESResultByTitleLikeOrContentLike(String title, String content, Pageable pageable);
    Page<ESResult> searchESResultByWebsiteLike(String website,Pageable pageable);
    Page<ESResult> searchESResultByTypeLike(String type,Pageable pageable);
    Page<ESResult> findESResultById(String id,Pageable pageable);

    //这两个没有加like的模糊好用
    Page<ESResult> findESResultByTitleOrContent(String title, String content, Pageable pageable);
    Page<ESResult> searchESResultByTitleOrContent(String title, String content, Pageable pageable);

}