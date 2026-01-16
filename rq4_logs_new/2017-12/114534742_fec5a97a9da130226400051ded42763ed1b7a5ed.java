package commaweed.generate.service;

import org.apache.commons.text.RandomStringGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.commons.text.CharacterPredicates.DIGITS;
import static org.apache.commons.text.CharacterPredicates.LETTERS;

@Service
public class PayloadGeneratorService {

   private static final Logger LOGGER = LoggerFactory.getLogger(PayloadGeneratorService.class);

   private static Charset charset = Charset.forName("UTF-8");
   private static CharsetEncoder encoder = charset.newEncoder();
   private static CharsetDecoder decoder = charset.newDecoder();

   RandomStringGenerator wordGenerator = new RandomStringGenerator.Builder()
      .withinRange('0', 'z')
      .filteredBy(LETTERS, DIGITS)
      .build();

   Random randomizer = new Random();

   /**
    * Converts the given value to a ByteBuffer.
    * @param value The value to convert.
    * @return A ByteBuffer for the underlying value.
    */
   public ByteBuffer convertToByteBuffer(String value) {
      ByteBuffer result = null;

      if (value != null) {
         try {
            result =  encoder.encode(CharBuffer.wrap(value));
         } catch (Exception e) {
            LOGGER.error("Unable to encode value=[{}]!", value, e);
         }
      }

      return result;
   }

   /**
    * Converts the given ByteBuffer to a String.
    * @param buffer The buffer to convert.
    * @return A String represented by the ByteBuffer.
    */
   public String convertToString(ByteBuffer buffer) {
      String result = null;

      try {
         int currentBufferPosition = buffer.position();

         result = decoder.decode(buffer).toString();

         // reset the buffer position to the original state
         buffer.position(currentBufferPosition);
      } catch (Exception e) {
         LOGGER.error("Unable to decode ByteBuffer!", e);
      }

      return result;
   }

   public String createRandomWord(int maxLength) {
      if (maxLength < 1) maxLength = 1;
      return wordGenerator.generate(randomizer.nextInt(maxLength));
   }

   public String createRandomParagraph(int maxWords, int maxWordLength) {
      if (maxWords < 1) maxWords = 1;
      return IntStream.rangeClosed(1, randomizer.nextInt(maxWords))
         .mapToObj(i -> createRandomWord(maxWordLength))
         .collect(Collectors.joining(" "));
   }

   public String createRandomDocument(int maxParagraphs, int maxWords, int maxWordLength) {
      return IntStream.rangeClosed(1, randomizer.nextInt(maxParagraphs))
         .mapToObj(i -> createRandomParagraph(maxWords, maxWordLength))
         .collect(Collectors.joining("\n"));
   }

   // TODO: obtain a dictionary file with real words to randomize

}