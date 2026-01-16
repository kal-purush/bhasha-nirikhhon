package PlanQ.PlanQ.quiz;

import PlanQ.PlanQ.option.Option;
import PlanQ.PlanQ.option.OptionRepository;
import PlanQ.PlanQ.question.Question;
import PlanQ.PlanQ.question.QuestionRepository;
import PlanQ.PlanQ.report.Report;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.imageio.ImageIO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.model.Media;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeType;
import org.springframework.web.multipart.MultipartFile;

@Slf4j
@Service
@RequiredArgsConstructor
public class QuizService {

    private final OpenAiChatModel openAiChatModel;
    private final QuizRepository quizRepository;
    private final QuestionRepository questionRepository;
    private final OptionRepository optionRepository;
    private final ObjectMapper objectMapper;

    @Value("${quiz.generation.prompt}")
    private String prompt;

    @Transactional
    public void generateQuizFromPdf(MultipartFile file, Report report) throws IOException {

        List<BufferedImage> images = convertPdfToImages(file);

        List<Media> mediaList = new ArrayList<>();
        for (BufferedImage image : images) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(image, "jpeg", outputStream);

            Media media = new Media(
                MimeType.valueOf(MediaType.IMAGE_JPEG_VALUE),
                new InputStreamResource(new ByteArrayInputStream(outputStream.toByteArray()))
            );

            mediaList.add(media);
        }

        UserMessage userMessage = new UserMessage(prompt, mediaList);
        String aiResponse = openAiChatModel.call(userMessage);

//        log.info("AI 응답: {}", aiResponse);

        saveQuizDataFromResponse(aiResponse, report);

    }

    private List<BufferedImage> convertPdfToImages(MultipartFile file) throws IOException {
        List<BufferedImage> images = new ArrayList<>();

        try (PDDocument document = PDDocument.load(file.getInputStream())) {
            PDFRenderer pdfRenderer = new PDFRenderer(document);

            for (int page = 0; page < document.getNumberOfPages(); page++) {
                BufferedImage image = pdfRenderer.renderImageWithDPI(page, 300, ImageType.RGB);
                images.add(image);
            }
        }
        return images;
    }

    private void saveQuizDataFromResponse(String aiResponse, Report report) {
        try {
            if (aiResponse.contains("{")) {
                aiResponse = aiResponse.substring(aiResponse.indexOf("{"));
            }
            if (aiResponse.contains("}")) {
                aiResponse = aiResponse.substring(0, aiResponse.lastIndexOf("}") + 1);
            }
            log.info("AI 응답: {}", aiResponse);

            JsonNode rootNode = objectMapper.readTree(aiResponse);
            JsonNode quizzesNode = rootNode.get("quizzes");

            if (quizzesNode == null || !quizzesNode.isArray()) {
                log.error("AI 응답에서 'quizzes' 키가 없거나 배열 형식이 아닙니다.");
                throw new RuntimeException("AI 응답 데이터 처리 중 오류 발생");
            }

            List<LocalDateTime> reviewDates = List.of(
                LocalDateTime.now().plusDays(1),
                LocalDateTime.now().plusDays(3),
                LocalDateTime.now().plusDays(7),
                LocalDateTime.now().plusDays(14)
            );

            int index = 0;

            for (JsonNode quizNode : quizzesNode) {
                String title = quizNode.get("title").asText();
//                String category = quizNode.has("category") ? quizNode.get("category").asText() : "기타";
                JsonNode questionsNode = quizNode.get("questions");

                if (questionsNode == null || !questionsNode.isArray()) {
                    log.error("퀴즈 '{}'의 'questions' 키가 없거나 배열 형식이 아닙니다.", title);
                    throw new RuntimeException("AI 응답 데이터 처리 중 오류 발생");
                }

                Quiz quiz = Quiz.builder()
                    .category("직접 지정 or Todo 제목")
                    .title(title)
                    .reviewDate(reviewDates.get(index % reviewDates.size()))
                    .questionCnt(questionsNode.size())
                    .report(report)
                    .build();
                quizRepository.save(quiz);

                index++;

                for (JsonNode questionNode : questionsNode) {
                    String content = questionNode.get("content").asText();
                    String correctOption = String.valueOf(questionNode.get("correct").asInt());
                    int questionNum = questionNode.get("question_id").asInt();
                    JsonNode optionsNode = questionNode.get("options");

                    if (optionsNode == null || !optionsNode.isArray()) {
                        log.error("질문 '{}'의 'options' 키가 없거나 배열 형식이 아닙니다.", content);
                        throw new RuntimeException("AI 응답 데이터 처리 중 오류 발생");
                    }

                    Question question = Question.builder()
                        .content(content)
                        .correct(correctOption)
                        .questionNum(questionNum)
                        .quiz(quiz)
                        .build();
                    questionRepository.save(question);

                    for (JsonNode optionNode : optionsNode) {
                        String optionContent = optionNode.get("content").asText();

                        Option option = Option.builder()
                            .content(optionContent)
                            .question(question)
                            .build();
                        optionRepository.save(option);
                    }
                }
            }
        } catch (Exception e) {
            log.error("AI 응답 처리 중 오류 발생: {}", e.getMessage(), e);
            throw new RuntimeException("AI 응답 데이터 처리 중 오류 발생", e);
        }
    }


}