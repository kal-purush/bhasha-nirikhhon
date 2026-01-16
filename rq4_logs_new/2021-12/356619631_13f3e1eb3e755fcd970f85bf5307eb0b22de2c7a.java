package org.uresti.pozarreal.service.impl;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.uresti.pozarreal.model.Chip;
import org.uresti.pozarreal.repository.ChipsRepository;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

public class ChipsServiceImplTests {

    @Test
    public void givenAnEmptyList_whenGetChipsByHouse_thenListEmptyIsReturned() {
        // Given:
        ChipsRepository chipsRepository = Mockito.mock(ChipsRepository.class);

        ChipsServiceImpl chipsService = new ChipsServiceImpl(chipsRepository);

        List<Chip> chips = new LinkedList<>();

        String houseId = UUID.randomUUID().toString();
        Mockito.when(chipsRepository.getChipByHouse(houseId)).thenReturn(chips);

        // When:
        List<org.uresti.pozarreal.dto.Chip> chipList = chipsService.getChipsByHouse(houseId);

        // Then:
        Assertions.assertThat(chipList.isEmpty()).isTrue();
    }

    @Test
    public void givenAnListWithTwoElements_whenGetChipsByHouse_thenListWithTwoElementsIsReturned() {
        // Given:
        ChipsRepository chipsRepository = Mockito.mock(ChipsRepository.class);

        ChipsServiceImpl chipsService = new ChipsServiceImpl(chipsRepository);

        List<Chip> chips = new LinkedList<>();

        Chip chip1 = Chip.builder()
                .id("id1")
                .code("code1")
                .house("house1")
                .valid(true)
                .notes("hello")
                .build();

        Chip chip2 = Chip.builder()
                .id("id2")
                .code("code2")
                .house("house2")
                .valid(true)
                .notes("hello")
                .build();

        chips.add(chip1);
        chips.add(chip2);

        String houseId = UUID.randomUUID().toString();
        Mockito.when(chipsRepository.getChipByHouse(houseId)).thenReturn(chips);

        // When:
        List<org.uresti.pozarreal.dto.Chip> chipList = chipsService.getChipsByHouse(houseId);

        // Then:
        Assertions.assertThat(chipList.size()).isEqualTo(2);
        Assertions.assertThat(chipList.get(0).getId()).isEqualTo("id1");
        Assertions.assertThat(chipList.get(0).getCode()).isEqualTo("code1");
        Assertions.assertThat(chipList.get(0).getHouse()).isEqualTo("house1");
        Assertions.assertThat(chipList.get(0).isValid()).isTrue();
        Assertions.assertThat(chipList.get(0).getNotes()).isEqualTo("hello");

        Assertions.assertThat(chipList.get(1).getId()).isEqualTo("id2");
        Assertions.assertThat(chipList.get(1).getCode()).isEqualTo("code2");
        Assertions.assertThat(chipList.get(1).getHouse()).isEqualTo("house2");
        Assertions.assertThat(chipList.get(1).isValid()).isTrue();
        Assertions.assertThat(chipList.get(1).getNotes()).isEqualTo("hello");
    }

    @Test
    public void givenAnChipId_whenActivateChip_thenChipsIsActivated() {
        // Given:
        ChipsRepository chipsRepository = Mockito.mock(ChipsRepository.class);

        ChipsServiceImpl chipsService = new ChipsServiceImpl(chipsRepository);

        Chip chip = Chip.builder()
                .id("id")
                .code("code")
                .house("house")
                .valid(false)
                .notes("hello")
                .build();

        String chipId = UUID.randomUUID().toString();
        Mockito.when(chipsRepository.findById(chipId)).thenReturn(java.util.Optional.ofNullable(chip));
        assert chip != null;
        Mockito.when(chipsRepository.save(chip)).thenReturn(chip);

        // When:
        org.uresti.pozarreal.dto.Chip chipReturned = chipsService.activateChip(chipId);

        // Then:
        Assertions.assertThat(chipReturned).isNotNull();
        Assertions.assertThat(chipReturned.isValid()).isTrue();
        Assertions.assertThat(chipReturned.getId()).isEqualTo("id");
        Assertions.assertThat(chipReturned.getCode()).isEqualTo("code");
        Assertions.assertThat(chipReturned.getNotes()).isEqualTo("hello");
        Assertions.assertThat(chipReturned.getHouse()).isEqualTo("house");

        Mockito.verify(chipsRepository).save(chip);
        Mockito.verify(chipsRepository).findById(chipId);
    }

    @Test
    public void givenAnWrongChipId_whenActivateChip_thenThrowNoSuchElementException() {
        // Given:
        ChipsRepository chipsRepository = Mockito.mock(ChipsRepository.class);

        ChipsServiceImpl chipsService = new ChipsServiceImpl(chipsRepository);

        String chipId = UUID.randomUUID().toString();

        // When:
        // Then:
        Assertions.assertThatThrownBy(() -> chipsService.activateChip(chipId))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void givenAnChipId_whenDeactivateChip_thenChipsIsDeactivated() {
        // Given:
        ChipsRepository chipsRepository = Mockito.mock(ChipsRepository.class);

        ChipsServiceImpl chipsService = new ChipsServiceImpl(chipsRepository);

        Chip chip = Chip.builder()
                .id("id")
                .code("code")
                .house("house")
                .valid(true)
                .notes("hello")
                .build();

        String chipId = UUID.randomUUID().toString();
        Mockito.when(chipsRepository.findById(chipId)).thenReturn(java.util.Optional.ofNullable(chip));
        assert chip != null;
        Mockito.when(chipsRepository.save(chip)).thenReturn(chip);

        // When:
        org.uresti.pozarreal.dto.Chip chipReturned = chipsService.deactivateChip(chipId);

        // Then:
        Assertions.assertThat(chipReturned).isNotNull();
        Assertions.assertThat(chipReturned.isValid()).isFalse();
        Assertions.assertThat(chipReturned.getId()).isEqualTo("id");
        Assertions.assertThat(chipReturned.getCode()).isEqualTo("code");
        Assertions.assertThat(chipReturned.getNotes()).isEqualTo("hello");
        Assertions.assertThat(chipReturned.getHouse()).isEqualTo("house");

        Mockito.verify(chipsRepository).save(chip);
        Mockito.verify(chipsRepository).findById(chipId);
    }

    @Test
    public void givenAnWrongChipId_whenDeactivateChip_thenThrowNoSuchElementException() {
        // Given:
        ChipsRepository chipsRepository = Mockito.mock(ChipsRepository.class);

        ChipsServiceImpl chipsService = new ChipsServiceImpl(chipsRepository);

        String chipId = UUID.randomUUID().toString();

        // When:
        // Then:
        Assertions.assertThatThrownBy(() -> chipsService.deactivateChip(chipId))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void givenAChip_whenAddChip_thenChipIsAdded() {
        // Given:
        ChipsRepository chipsRepository = Mockito.mock(ChipsRepository.class);

        ChipsServiceImpl chipsService = new ChipsServiceImpl(chipsRepository);

        ArgumentCaptor<Chip> argumentCaptor = ArgumentCaptor.forClass(Chip.class);

        Chip chip = Chip.builder()
                .id("id")
                .code("code")
                .house("house")
                .valid(true)
                .notes("hello")
                .build();

        org.uresti.pozarreal.dto.Chip chipDto = org.uresti.pozarreal.dto.Chip.builder()
                .id("abc")
                .code("code")
                .house("house")
                .valid(true)
                .notes("hello")
                .build();

        Mockito.when(chipsRepository.save(argumentCaptor.capture())).thenReturn(chip);

        // When:
        org.uresti.pozarreal.dto.Chip addChip = chipsService.addChip(chipDto);

        // Then:
        Chip parameter = argumentCaptor.getValue();

        Assertions.assertThat(addChip).isNotNull();
        Assertions.assertThat(addChip.isValid()).isTrue();
        Assertions.assertThat(addChip.getCode()).isEqualTo("code");
        Assertions.assertThat(addChip.getNotes()).isEqualTo("hello");
        Assertions.assertThat(addChip.getHouse()).isEqualTo("house");
        Assertions.assertThat(addChip.getId()).isNotEqualTo(parameter.getId());
    }

    @Test
    public void givenAnChipId_whenDeleteChip_thenChipsIsDeletedAndReturned() {
        // Given:
        ChipsRepository chipsRepository = Mockito.mock(ChipsRepository.class);

        ChipsServiceImpl chipsService = new ChipsServiceImpl(chipsRepository);

        Chip chip = Chip.builder()
                .id("id")
                .code("code")
                .house("house")
                .valid(false)
                .notes("hello")
                .build();

        String chipId = UUID.randomUUID().toString();
        Mockito.when(chipsRepository.findById(chipId)).thenReturn(java.util.Optional.ofNullable(chip));

        // When:
        org.uresti.pozarreal.dto.Chip deleteChip = chipsService.deleteChip(chipId);

        // Then:
        Assertions.assertThat(deleteChip).isNotNull();
        Assertions.assertThat(deleteChip.isValid()).isFalse();
        Assertions.assertThat(deleteChip.getId()).isEqualTo("id");
        Assertions.assertThat(deleteChip.getCode()).isEqualTo("code");
        Assertions.assertThat(deleteChip.getNotes()).isEqualTo("hello");
        Assertions.assertThat(deleteChip.getHouse()).isEqualTo("house");

        Mockito.verify(chipsRepository).deleteById(chipId);
    }

    @Test
    public void givenAnWrongChipId_whenDeleteChip_thenThrowNoSuchElementException() {
        // Given:
        ChipsRepository chipsRepository = Mockito.mock(ChipsRepository.class);

        ChipsServiceImpl chipsService = new ChipsServiceImpl(chipsRepository);

        String chipId = UUID.randomUUID().toString();

        // When:
        // Then:
        Assertions.assertThatThrownBy(()-> chipsService.deleteChip(chipId))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void given_whenGetChipsWithRealStatus_then() {
        // Given:
        ChipsRepository chipsRepository = Mockito.mock(ChipsRepository.class);

        ChipsServiceImpl chipsService = new ChipsServiceImpl(chipsRepository);

        List<Chip> chips = new LinkedList<>();

        Chip chip1 = Chip.builder()
                .id("id1")
                .code("code1")
                .house("house1")
                .valid(true)
                .notes("hello")
                .build();

        Chip chip2 = Chip.builder()
                .id("id2")
                .code("code2")
                .house("house2")
                .valid(true)
                .notes("hello")
                .build();

        chips.add(chip1);
        chips.add(chip2);

        Mockito.when(chipsRepository.getChipsWithRealStatus()).thenReturn(chips);

        // When:
        List<Chip> chipsWithRealStatus = chipsService.getChipsWithRealStatus();

        // Then:
        Assertions.assertThat(chipsWithRealStatus.size()).isEqualTo(2);
        Assertions.assertThat(chipsWithRealStatus.get(0).getId()).isEqualTo("id1");
        Assertions.assertThat(chipsWithRealStatus.get(0).getCode()).isEqualTo("code1");
        Assertions.assertThat(chipsWithRealStatus.get(0).getHouse()).isEqualTo("house1");
        Assertions.assertThat(chipsWithRealStatus.get(0).isValid()).isTrue();
        Assertions.assertThat(chipsWithRealStatus.get(0).getNotes()).isEqualTo("hello");

        Assertions.assertThat(chipsWithRealStatus.get(1).getId()).isEqualTo("id2");
        Assertions.assertThat(chipsWithRealStatus.get(1).getCode()).isEqualTo("code2");
        Assertions.assertThat(chipsWithRealStatus.get(1).getHouse()).isEqualTo("house2");
        Assertions.assertThat(chipsWithRealStatus.get(1).isValid()).isTrue();
        Assertions.assertThat(chipsWithRealStatus.get(1).getNotes()).isEqualTo("hello");
    }
}