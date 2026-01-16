/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package foxunrpi;

import com.pi4j.io.gpio.GpioPinDigitalInput;
import com.pi4j.io.gpio.event.GpioPinDigitalStateChangeEvent;
import com.pi4j.io.gpio.event.GpioPinListenerDigital;

/**
 *
 * @author pitte
 */
public class FourWaySwitch {
    private GpioPinDigitalInput _input1;
    private GpioPinDigitalInput _input2;
    private GpioPinDigitalInput _input3;
    private GpioPinDigitalInput _input4;
    private int _currentInput = -1;
    
    
    public FourWaySwitch(GpioPinDigitalInput input1, GpioPinDigitalInput input2, GpioPinDigitalInput input3, GpioPinDigitalInput input4, int defaultInput){
        _input1 = input1;
        _input2 = input2;
        _input3 = input3;
        _input4 = input4;
        
        initialize(defaultInput);
        
        input1.addListener((GpioPinListenerDigital) (GpioPinDigitalStateChangeEvent event) -> {
            if(event.getState().isHigh()){
                _currentInput = 1;
            }
        });
        
        input2.addListener((GpioPinListenerDigital) (GpioPinDigitalStateChangeEvent event) -> {
            if(event.getState().isHigh()){
                _currentInput = 2;
            }
        });
                
        input3.addListener((GpioPinListenerDigital) (GpioPinDigitalStateChangeEvent event) -> {
            if(event.getState().isHigh()){
                _currentInput = 3;
            }
        });
                        
        input4.addListener((GpioPinListenerDigital) (GpioPinDigitalStateChangeEvent event) -> {
            if(event.getState().isHigh()){
                _currentInput = 4;
            }
        });
    }
    
    public int getCurrentInput(){
        return _currentInput;
    }
    
    private void initialize(int defaultInput){
        if(_input1.isHigh()){
            _currentInput = 1;
        }
        else if(_input2.isHigh()){
            _currentInput = 2;
        }
        else if(_input3.isHigh()){
            _currentInput = 3;
        }
        else if(_input4.isHigh()){
            _currentInput = 4;
        }
        else{
            _currentInput = defaultInput;
        }
    }
}