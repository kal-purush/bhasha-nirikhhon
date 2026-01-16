package com.mower.application;

import com.mower.console.ConsoleApplication;
import com.mower.usecase.ExecuteMowerCommandsInPlateauUseCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class MowerAppShould {
  @Mock
  ConsoleApplication consoleApplicationMocked;

  @Test
  void main() {
    doNothing().when(consoleApplicationMocked).start(any(ExecuteMowerCommandsInPlateauUseCase.class));
    MowerApp.initForTestPurposes(consoleApplicationMocked);
    MowerApp.main();
    verify(consoleApplicationMocked).start(any(ExecuteMowerCommandsInPlateauUseCase.class));
  }
}