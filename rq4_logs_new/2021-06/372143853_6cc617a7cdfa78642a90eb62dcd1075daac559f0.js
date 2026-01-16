import Counter from './Counter';
import HoverCounter from './HoverCounter';
import ThemeContext from './ThemeContext';

export default function Content() {
    return (
        <div>
            <Counter>
                {(count, incrementCount) => (
                    <ThemeContext.Consumer>
                        {(theme) => (
                            <HoverCounter
                                count={count}
                                theme={theme}
                                incrementCount={incrementCount}
                            />
                        )}
                    </ThemeContext.Consumer>
                )}
            </Counter>
        </div>
    );
}