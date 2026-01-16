/*******************************************
 *
 * Author: Ryan McGuire
 * Date: 10/12/2018
 * Step 5
 *
 *
 *******************************************/

package productionline;

public class AudioPlayer extends Product implements MultimediaControl {

  private String audioSpecification;
  private ItemType mediaType;

  /***
   *
  * This constructor is setting the name of the product through the parent constructor,
  * setting the audioSpecification field and declaring the mediaTpe as AUDIO.
  * @param name The name of the manufacturer being passed to the parent constructor to set it.
  * @param audioSpecification setting the audioSpecification field with the
   * specification of the audio
   *
  ***/
  public AudioPlayer(String name, String audioSpecification) {

    super(name);

    this.audioSpecification = audioSpecification;

    mediaType = ItemType.AUDIO;
  }

  @Override
  public void play() {

    System.out.println("Playing");
  }

  @Override
  public void stop() {

    System.out.println("Stopped");
  }

  @Override
  public void previous() {

    System.out.println("Previous");
  }

  @Override
  public void next() {

    System.out.println("Next");
  }

  @Override
  public String toString() {

    return super.toString() +
        "Audio Specification : " + audioSpecification + "\n" +
        "Media Type : " + mediaType;
  }
}