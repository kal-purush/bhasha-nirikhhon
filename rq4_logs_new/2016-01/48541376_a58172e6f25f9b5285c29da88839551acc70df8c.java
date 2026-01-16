/*
 * The MIT License
 *
 * Copyright 2016 andrewnyhus.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package java2dscrollinguniverse.Model.actors;

/**
 *
 * @author andrewnyhus
 */
public class ActorLabel {
    
    private String labelText;
    private PositionOfLabel position;
 
    public ActorLabel(String text, PositionOfLabel position){
        this.labelText = text;
        this.position = position;
    }

    
    /**
     * @return the labelText
     */
    public String getLabelText() {
        return labelText;
    }

    /**
     * @param labelText the labelText to set
     */
    public void setLabelText(String labelText) {
        this.labelText = labelText;
    }

    
    /**
     * @return the position
     */
    public PositionOfLabel getPosition() {
        return position;
    }

    /**
     * @param position the position to set
     */
    public void setPosition(PositionOfLabel position) {
        this.position = position;
    }
    
    
    
    
    public enum PositionOfLabel{
        LEFT_OF_TOP(0), MIDDLE_OF_TOP(1), RIGHT_OF_TOP(2),
        TOP_OF_RIGHT(3), MIDDLE_OF_RIGHT(4), BOTTOM_OF_RIGHT(5), 
        RIGHT_OF_BOTTOM(6), MIDDLE_OF_BOTTOM(9), LEFT_OF_BOTTOM(10);
        
        private int value;
        
        private PositionOfLabel(int value){
            this.value = value;
        }

        
        /**
         * @return the value
         */
        public int getValue() {
            return value;
        }
        
        
    
    }
    
}