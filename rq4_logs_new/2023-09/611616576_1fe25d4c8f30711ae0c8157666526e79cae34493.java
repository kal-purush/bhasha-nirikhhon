package com.seproject.admin.post.application;

import com.seproject.account.account.domain.Account;
import com.seproject.account.utils.SecurityUtils;
import com.seproject.admin.post.controller.dto.PostRequest.AdminPostRetrieveCondition;
import com.seproject.admin.post.controller.dto.PostResponse.DeletedPostResponse;
import com.seproject.admin.post.controller.dto.PostResponse.PostRetrieveResponse;
import com.seproject.admin.post.service.AdminPostService;
import com.seproject.board.menu.domain.Menu;
import com.seproject.board.menu.service.MenuService;
import com.seproject.board.post.domain.repository.AdminPostSearchRepository;
import com.seproject.board.post.service.PostService;
import com.seproject.error.errorCode.ErrorCode;
import com.seproject.error.exception.CustomAccessDeniedException;
import com.seproject.error.exception.CustomAuthenticationException;
import com.seproject.error.exception.InvalidAuthorizationException;
import com.seproject.error.exception.NoSuchResourceException;
import com.seproject.board.menu.domain.Category;
import com.seproject.member.domain.BoardUser;
import com.seproject.board.post.domain.model.Post;
import com.seproject.board.menu.domain.repository.CategoryRepository;
import com.seproject.board.common.domain.repository.ReportRepository;
import com.seproject.member.domain.repository.BoardUserRepository;
import com.seproject.board.post.domain.repository.PostRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.file.AccessDeniedException;
import java.util.List;

import static com.seproject.board.common.utils.AppServiceHelper.findByIdOrThrow;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class AdminPostAppService {

    private final CategoryRepository categoryRepository;

    private final AdminPostService adminPostService;
    private final PostService postService;
    private final AdminPostSearchRepository adminPostSearchRepository;

    public Page<PostRetrieveResponse> findPostList(AdminPostRetrieveCondition condition, int page, int perPage) {
//        Account account = SecurityUtils.getAccount()
//                .orElseThrow(() -> new InvalidAuthorizationException(ErrorCode.NOT_LOGIN));
//
//        boolean isAdmin = account.getAuthorities().stream().anyMatch(authority -> authority.getAuthority().equals("ROLE_ADMIN"));
//
//        if(!isAdmin){
//            throw new InvalidAuthorizationException(ErrorCode.ACCESS_DENIED);
//        }

        return adminPostSearchRepository.findPostListByCondition(condition, PageRequest.of(page, perPage));
    }

    public Page<DeletedPostResponse> findDeletedPostList(Pageable pageable){
//        Account account = SecurityUtils.getAccount()
//                .orElseThrow(() -> new InvalidAuthorizationException(ErrorCode.NOT_LOGIN));
//
//        boolean isAdmin = account.getAuthorities().stream().anyMatch(authority -> authority.getAuthority().equals("ROLE_ADMIN"));
//
//        if(!isAdmin){
//            throw new InvalidAuthorizationException(ErrorCode.ACCESS_DENIED);
//        }

        return adminPostSearchRepository.findDeletedPostList(pageable);
    }

    @Transactional
    public void enrollPin(Long postId) {
        changePostPinState(postId,true);
    }

    @Transactional
    public void cancelPin(Long postId) {
        changePostPinState(postId,false);
    }

    private void changePostPinState(Long postId, boolean state) {
        Post post = postService.findById(postId);
        Category category = post.getCategory();
        Account account = SecurityUtils.getAccount()
                .orElseThrow(() -> new CustomAuthenticationException(ErrorCode.NOT_LOGIN, null));

        boolean manageable = category.manageable(account.getRoles());

        if (manageable) {
            post.changePin(state);
            return;
        }

        throw new CustomAccessDeniedException(ErrorCode.ACCESS_DENIED,null);
    }

    @Transactional
    public void restorePost(Long postId){
//        Account account = SecurityUtils.getAccount()
//                .orElseThrow(() -> new InvalidAuthorizationException(ErrorCode.NOT_LOGIN));
//
//        boolean isAdmin = account.getAuthorities().stream().anyMatch(authority -> authority.getAuthority().equals("ROLE_ADMIN"));
//
//        if(!isAdmin){
//            throw new InvalidAuthorizationException(ErrorCode.ACCESS_DENIED);
//        }

        Post post = postService.findById(postId);
        adminPostService.restore(post);
    }

    @Transactional
    public void restoreBulkPost(List<Long> postIds) {
//        Account account = SecurityUtils.getAccount()
//                .orElseThrow(() -> new InvalidAuthorizationException(ErrorCode.NOT_LOGIN));
//
//        boolean isAdmin = account.getAuthorities().stream().anyMatch(authority -> authority.getAuthority().equals("ROLE_ADMIN"));
//
//        if(!isAdmin){
//            throw new InvalidAuthorizationException(ErrorCode.ACCESS_DENIED);
//        }

        adminPostService.restore(postIds);
    }

    @Transactional
    public void deleteBulkPost(List<Long> postIds, boolean isPermanent) {
//        Account account = SecurityUtils.getAccount()
//                .orElseThrow(() -> new InvalidAuthorizationException(ErrorCode.NOT_LOGIN));
//
//        boolean isAdmin = account.getAuthorities().stream().anyMatch(authority -> authority.getAuthority().equals("ROLE_ADMIN"));
//
//        if(!isAdmin){
//            throw new InvalidAuthorizationException(ErrorCode.ACCESS_DENIED);
//        }
        adminPostService.deleteAllByIds(postIds,isPermanent);
    }

    @Transactional
    public void migratePost(Long fromCategoryId, Long toCategoryId){

        Category from = categoryRepository.findById(fromCategoryId)
                .orElseThrow(() -> new NoSuchResourceException(ErrorCode.NOT_EXIST_CATEGORY));
        Category to = categoryRepository.findById(toCategoryId)
                .orElseThrow(() -> new NoSuchResourceException(ErrorCode.NOT_EXIST_CATEGORY));

        adminPostService.changeCategory(from, to);
    }

}