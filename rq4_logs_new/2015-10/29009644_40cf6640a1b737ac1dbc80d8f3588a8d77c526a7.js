Meteor.startup(() => {
  const applyThemeStyles = (theme = []) => {
    const rules = [];
    for (const item of theme) {
      rules.push(`

[btn~="${item.location}"] {
  background-color: ${item.color};
}

[btn~="${item.location}"]:active:enabled {
  box-shadow: inset 0px 2px 6px rgba(0, 0, 0, 0.5), 0px 1px 0px rgba(255, 255, 255, 0.4);
  text-shadow: 0px 1px 0px rgba(0, 0, 0, 0.2);
  background: ${item.color};
  transition-property: background;
  transition-duration: .5s;
}

[btn~="${item.location}"][btn~="outline"]  {
  background-color: inherit;
  color: ${item.color};
  border-color: ${item.color};
}

[btn~="${item.location}"][btn~="outline"]:active:enabled {
  box-shadow: inset 0px 1px 3px rgba(0, 0, 0, 0.3);
  border-color: ${item.color};
  text-shadow: 0px 1px 0px rgba(0, 0, 0, 0.4);
  color: ${item.color};
}

[btn~="${item.location}"][btn~="outline"][is-activated] {
    background-color: ${item.color};
    color: inherit;
}

[btn~="${item.location}"][btn~="transparent"] {
  background-color: hsla(0, 0%, 0%, 0);
  color: ${item.color};
}

[btn~="${item.location}"][btn~="transparent"]:active:enabled {
  background-color: hsla(0, 0%, 0%, 0);
  box-shadow: none;
  text-shadow: 0px 1px 0px rgba(0, 0, 0, 0.2);
  color: ${item.color};
}

[btn~="${item.location}"][btn~="transparent"][is-activated] {
    background-color: ${item.color};
    color: white;
    cursor: default;
}

[toggle~="${item.location}"] input:checked + .track {
  border-color: ${item.color};
  background-color: ${item.color};
}
`
      );
    }
    ooInjectStyle(rules, 'buttons');
  };

  Tracker.autorun(() => {
    applyThemeStyles(Session.get('ooTheme'));
  });
});