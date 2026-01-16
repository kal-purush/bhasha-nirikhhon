package post;

import lombok.RequiredArgsConstructor;
import spring.annotation.Controller;
import spring.annotation.GetMapping;

@Controller
@RequiredArgsConstructor
public class PostController {

    private final PostService postService;

    @GetMapping("/posts/add")
    public void add() {
        System.out.println("PostController.add");
    }

    @GetMapping("/posts/remove")
    public void remove() {
        System.out.println("PostController.remove");
    }

    @GetMapping("/posts/edit")
    public void edit() {
        System.out.println("PostController.edit");
    }

    @GetMapping("/posts/view")
    public void view() {
        System.out.println("PostController.view");
    }
}