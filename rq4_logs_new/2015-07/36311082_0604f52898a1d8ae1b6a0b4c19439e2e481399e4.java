/*
 * Copyright 2015 by the Metanome project
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
 * limitations under the License.
 */

package de.metanome.backend.input;

import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;

/**
 * Generates {@link MaskingRelationalInput}s.
 * @author Jakob Zwiener
 * @see MaskingRelationalInput
 */
public class MaskingRelationalInputGenerator implements RelationalInputGenerator {

  protected RelationalInputGenerator inputGenerator;
  protected int[] mask;

  public MaskingRelationalInputGenerator(RelationalInputGenerator inputGenerator, int... mask) {
    this.inputGenerator = inputGenerator;
    this.mask = mask;
  }

  @Override public RelationalInput generateNewCopy() throws InputGenerationException {
    try {
      return new MaskingRelationalInput(inputGenerator.generateNewCopy(), mask);
    }
    catch (InvalidMaskException e) {
      throw new InputGenerationException("The mask was invalid and the input could not be generated.", e);
    }
  }
}