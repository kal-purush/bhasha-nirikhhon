package project.vilsoncake.BookOnlineStore.utils;

import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;
import project.vilsoncake.BookOnlineStore.entity.BookEntity;
import project.vilsoncake.BookOnlineStore.repository.BookRepository;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static project.vilsoncake.BookOnlineStore.constant.MessageConst.*;
import static project.vilsoncake.BookOnlineStore.constant.NameConst.*;
import static project.vilsoncake.BookOnlineStore.constant.PatternConst.REGEX_NAME_PATTERN;
import static project.vilsoncake.BookOnlineStore.constant.PatternConst.REGEX_ONLY_LETTERS_PATTERN;

@Component
public class BookUtils {

    private final BookRepository bookRepository;

    public BookUtils(BookRepository bookRepository) {
        this.bookRepository = bookRepository;
    }

    public Map<String, String> isValidBook(BookEntity book, String issueYear, String page, String startCount, MultipartFile avatar) {
        Map<String, String> result = new HashMap<>();
        result.put("nameError", isValidName(book.getName()) ? "" : BOOK_NAME_INVALID_MESSAGE);
        result.put("authorError", isValidAuthor(book.getAuthor()) ? "" : BOOK_AUTHOR_INVALID_MESSAGE);
        result.put("publisherError", isValidPublisher(book.getPublisher()) ? "" : BOOK_PUBLISHER_INVALID_MESSAGE);
        result.put("issueYearError", isValidIssueYear(issueYear) ? "" : BOOK_ISSUE_YEAR_INVALID_MESSAGE);
        result.put("pageError", isValidPage(page) ? "" : BOOK_PAGE_INVALID_MESSAGE);
        result.put("languageError", isValidLanguage(book.getLanguage()) ? "" : LANGUAGE_INVALID_MESSAGE);
        result.put("bindingError", isValidBinding(book.getBinding()) ? "" : BINDING_INVALID_MESSAGE);
        result.put("isbnError", isValidIsbn(book.getIsbn()) ? "" : ISBN_INVALID_MESSAGE);
        result.put("categoryError", isValidCategory(book.getCategory()) ? "" : CATEGORY_INVALID_MESSAGE);
        result.put("descriptionError", isValidDescription(book.getDescription()) ? "" : DESCRIPTION_INVALID_MESSAGE);
        result.put("startCountError", isValidStartCount(startCount) ? "" : START_COUNT_INVALID_MESSAGE);
        result.put("fileError", isValidFile(avatar) ? "" : FILE_EXTENSION_INVALID_MESSAGE);
        result.put("bookExistError", bookRepository.findByIsbn(book.getIsbn()) == null ? "" : BOOK_ALREADY_EXIST_MESSAGE);
        return result;
    }

    public Map<String, String> bookToMap(BookEntity book, String issueYear, String page, String startCount) {
        Map<String, String> result = new HashMap<>();
        result.put("name", book.getName());
        result.put("author", book.getAuthor());
        result.put("publisher", book.getPublisher());
        result.put("issueYear", issueYear);
        result.put("page", page);
        result.put("language", book.getLanguage());
        result.put("binding", book.getBinding());
        result.put("isbn", book.getIsbn());
        result.put("category", book.getCategory());
        result.put("description", book.getDescription());
        result.put("startCount", startCount);
        return result;
    }

    public boolean isValidName(String name) {
        return name.length() > 3 && name.length() < 30;
    }

    public boolean isValidAuthor(String author) {
        String[] fullName = author.split(" ");
        Pattern pattern = Pattern.compile(REGEX_NAME_PATTERN);

        // If author not format <name> <surname>
        if (fullName.length != 2) return false;

        for (String name : fullName) {
            Matcher matcher = pattern.matcher(name);
            if (!matcher.find()) return false;
        }
        return true;
    }

    public boolean isValidPublisher(String publisher) {
        return publisher.length() > 3 && publisher.length() < 30;
    }

    public boolean isValidIssueYear(String year) {
        if (year == null) return false;

        try {
            return Integer.parseInt(year) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean isValidPage(String page) {
        if (page == null) return false;

        try {
            return Integer.parseInt(page) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean isValidLanguage(String language) {
        Pattern pattern = Pattern.compile(REGEX_ONLY_LETTERS_PATTERN);
        Matcher matcher = pattern.matcher(language);
        return matcher.find();
    }

    public boolean isValidBinding(String binding) {
        for (String admissibleBinding : BINDINGS_LIST)
            if (binding.equalsIgnoreCase(admissibleBinding)) return true;

        return false;
    }

    public boolean isValidDescription(String description) {
        return description.length() > 3 && description.split(" ").length < 50;
    }

    public boolean isValidIsbn(String isbn) {
        // Get digits count
        int digitsCount = 0;
        for (int i = 0; i < isbn.length(); i++)
            if (Character.isDigit(isbn.charAt(i))) digitsCount++;

        return digitsCount == 10 || digitsCount == 13;
    }

    public boolean isValidCategory(String category) {
        for (String admissibleCategory : BOOK_CATEGORIES)
            if (category.equalsIgnoreCase(admissibleCategory)) return true;

        return false;
    }

    public boolean isValidStartCount(String startCount) {
        if (startCount == null) return false;

        try {
            return Integer.parseInt(startCount) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public boolean isValidFile(MultipartFile file) {
        if (file == null) return false;

        String[] fileWithExtension = file.getOriginalFilename().split("\\.");
        int lastIndex = fileWithExtension.length - 1;

        if (fileWithExtension.length < 2) return false;

        for (String extension : FILE_EXTENSIONS)
            if (extension.equalsIgnoreCase(fileWithExtension[lastIndex])) return true;

        return false;
    }
}