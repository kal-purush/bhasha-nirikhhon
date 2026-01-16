using System;
using System.IO;
using DIKUArcade.Graphics;

namespace pong_wars {
    public enum Element {
        day,
        night,
    }

    static public class ElementControl {
        static public IBaseImage GetBlockColor(Element elment) {
            switch(elment) {
                case Element.day:
                    return new Image(Path.Combine("Assets", "Images", "blue-block.png"));
                case Element.night:
                    return new Image(Path.Combine("Assets", "Images", "green-block.png"));
                default:
                    throw new ArgumentException("No match");
            }
        }
        static public IBaseImage GetBallColor(Element elment) {
            switch(elment) {
                case Element.day:
                    return new Image(Path.Combine("Assets", "Images", "blue-ball.png"));
                case Element.night:
                    return new Image(Path.Combine("Assets", "Images", "green-ball.png"));
                default:
                    throw new ArgumentException("No match");
            }
        }
    }

}