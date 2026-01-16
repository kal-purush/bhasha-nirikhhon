
package com.bernardomg.tabletop.dreadball.build.dbx.model;

public final class ImmutableOptionGroup implements OptionGroup {

    private final String           name;

    private final Iterable<Option> options;

    public ImmutableOptionGroup(final String name,
            final Iterable<Option> options) {
        super();

        this.name = name;
        this.options = options;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final Iterable<Option> getOptions() {
        return options;
    }

}