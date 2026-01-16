/*
 * Copyright 2014 - 2017 Cognizant Technology Solutions
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 */
package com.cognizant.cognizantits.engine.commands.mobile.nativ;

import com.cognizant.cognizantits.engine.constants.SystemDefaults;
import com.cognizant.cognizantits.engine.core.CommandControl;
import com.cognizant.cognizantits.engine.execution.exception.ForcedException;
import com.cognizant.cognizantits.engine.execution.exception.element.ElementException;
import com.cognizant.cognizantits.engine.support.Status;
import com.cognizant.cognizantits.engine.support.methodInf.Action;
import com.cognizant.cognizantits.engine.support.methodInf.InputType;
import com.cognizant.cognizantits.engine.support.methodInf.ObjectType;
import io.appium.java_client.AppiumDriver;
import io.appium.java_client.MobileDriver;
import io.appium.java_client.MultiTouchAction;
import io.appium.java_client.TouchAction;
import io.appium.java_client.android.AndroidDriver;
import io.appium.java_client.ios.IOSDriver;
import io.appium.java_client.touch.LongPressOptions;
import io.appium.java_client.touch.TapOptions;
import io.appium.java_client.touch.WaitOptions;
import io.appium.java_client.touch.offset.ElementOption;
import io.appium.java_client.touch.offset.PointOption;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WaitFor extends MobileNativeCommand {

    public WaitFor(CommandControl cc) {
        super(cc);
    }

    private int getWaitTime() {
        if (Condition != null && Condition.matches("[0-9]+")) {
            return Integer.valueOf(Condition);
        } else {
            return SystemDefaults.waitTime.get();
        }
    }

    @Action(object = ObjectType.MOBILE, desc = "Wait for text present", input = InputType.YES)
    public void waitForTextPresent() {
        int time = getWaitTime();
        WebDriverWait wait = new WebDriverWait(Driver, time);
        try {
            if (Driver instanceof AndroidDriver) {
                wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath("//*[contains(@text, '" + Data + "')]")));
            } else if (Driver instanceof IOSDriver) {
                wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath("//*[contains(@label, '" + Data + "')]")));
            }
            Report.updateTestLog(Action, "Detected text: " + Data, Status.PASS);
        }
        catch (Exception ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.OFF, null, ex);
            throw new ForcedException(Action, ex.getMessage());
        }
    }
}