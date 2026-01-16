import { fireEvent, render, screen } from '@testing-library/react';
import { Synth } from './Synthesizer.js';
import * as Tone from 'tone';

describe("Synth Component Tests", () => {
    test("Correctly Renders Component Without Crashing", () => {
        render(<Synth input={"Paused"} tone={Tone}></Synth>);
        const buttons = screen.getAllByRole('button');
        expect(buttons.length).toEqual(5);
    });

    test("User Input On Range Slider Changes Waveform", () => {
        render(<Synth input={"Paused"} tone={Tone}></Synth>);
        const slider = screen.getByTestId('r');

        fireEvent.change(slider, { target: { value: 0 } });
        expect(screen.getByText("Sine")).toBeInTheDocument();

        fireEvent.change(slider, { target: { value: 1 } });
        expect(screen.getByText("Triangle")).toBeInTheDocument();

        fireEvent.change(slider, { target: { value: 2 } });
        expect(screen.getByText("Sawtooth")).toBeInTheDocument();

        fireEvent.change(slider, { target: { value: 3 } });
        expect(screen.getByText("Square")).toBeInTheDocument();
        expect(screen.queryByText("Sine")).not.toBeInTheDocument();
    })
})





