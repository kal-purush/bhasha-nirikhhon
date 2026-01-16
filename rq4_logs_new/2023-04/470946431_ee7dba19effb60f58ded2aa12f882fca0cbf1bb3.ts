import SlimSelect from "slim-select";

const ddlClientArray: SlimSelect[] = [];
const ddlClientContactArray: SlimSelect[] = [];
export const ddlClientInitialise = () => {
  ddlClientArray.forEach((element) => {
    element.destroy();
  });
  const slimSelectArray = document.querySelectorAll(
    ".ddlClientID"
  ) as NodeListOf<HTMLSelectElement>;
  slimSelectArray.forEach((element: HTMLSelectElement) => {
    if (element.dataset.ssid === undefined && element.localName === "select") {
      createContentLocation(element);
      ddlClientArray.push(
        new SlimSelect({
          select: element,
          events: {
            afterChange: () => {
              contactLoad(element.value);
            },
          },
          settings: {
            allowDeselect: true,
            placeholderText: "Please select...",
            searchText: "Sorry nothing to see here",
            searchPlaceholder: "Search",
            searchHighlight: true,
            contentLocation: document.getElementById(
              "contentLocation" + element.id
            ),
          },
        })
      );
    }
  });
};
export const ddlClientContactInitialise = () => {
  ddlClientContactArray.forEach((element) => {
    element.destroy();
  });
  const slimSelectArray = document.querySelectorAll(
    ".ddlClientContactID"
  ) as NodeListOf<HTMLSelectElement>;
  slimSelectArray.forEach((element: HTMLSelectElement) => {
    if (element.dataset.ssid === undefined && element.localName === "select") {
      createContentLocation(element);
      ddlClientContactArray.push(
        new SlimSelect({
          select: element,
          events: {
            afterChange: () => {
              PopulateContactAddresses(element.value);
            },
          },
          settings: {
            allowDeselect: true,
            placeholderText: "Please select...",
            searchText: "Sorry nothing to see here",
            searchPlaceholder: "Search",
            searchHighlight: true,
            contentLocation: document.getElementById(
              "contentLocation" + element.id
            ),
          },
        })
      );
    }
  });
};
export const ddlLogTypeInitialise = () => {
  const slimSelectArray = document.querySelectorAll(
    ".ddlLogContactTypeID"
  ) as NodeListOf<HTMLSelectElement>;
  slimSelectArray.forEach((element: HTMLSelectElement) => {
    if (element.dataset.ssid === undefined && element.localName === "select") {
      createContentLocation(element);
      new SlimSelect({
        select: element,
        settings: {
          allowDeselect: true,
          placeholderText: "Please select...",
          searchText: "Sorry nothing to see here",
          searchPlaceholder: "Search",
          searchHighlight: true,
          contentLocation: document.getElementById(
            "contentLocation" + element.id
          ),
        },
      });
    }
  });
};
export const ddlGenericInitialise = () => {
  const slimSelectArray = document.querySelectorAll(
    ".ddlGeneric"
  ) as NodeListOf<HTMLSelectElement>;
  slimSelectArray.forEach((element: HTMLSelectElement) => {
    if (element.dataset.ssid === undefined && element.localName === "select") {
      createContentLocation(element);
      new SlimSelect({
        select: element,
        settings: {
          allowDeselect: true,
          placeholderText: "Please select...",
          searchText: "Sorry nothing to see here",
          searchPlaceholder: "Search",
          searchHighlight: true,
          contentLocation: document.getElementById(
            "contentLocation" + element.id
          ),
        },
      });
    }
  });
};
export const ddlStyledInitialise = () => {
  const slimSelectArray = document.querySelectorAll(
    ".ddlStyled"
  ) as NodeListOf<HTMLSelectElement>;
  slimSelectArray.forEach((element: HTMLSelectElement) => {
    if (element.dataset.ssid === undefined && element.localName === "select") {
      createContentLocation(element);
      new SlimSelect({
        select: element,
        settings: {
          allowDeselect: true,
          placeholderText: "Please select...",
          searchText: "Sorry nothing to see here",
          searchPlaceholder: "Search",
          searchHighlight: true,
          contentLocation: document.getElementById(
            "contentLocation" + element.id
          ),
        },
      });
    }
  });
};
const PopulateContacts = (value: number) => {
  const ddlClientContactID = document.querySelector(
    ".ddlClientContactID"
  ) as HTMLSelectElement;
  if (ddlClientContactID != null) {
    const selected: string = getSelected(ddlClientContactID);
    ddlClientContactID.innerHTML = "";
    const hidGetClientContacts = document.getElementById(
      "hidGetClientContacts"
    ) as HTMLFormElement;
    fetch(
      hidGetClientContacts.value.toString() +
        "?id=" +
        encodeURIComponent(value),
      {
        method: "POST",
      }
    )
      .then((response) => {
        return response.json();
      })
      .then(function (result) {
        result.forEach((elem: any) => {
          createOption(elem, selected, ddlClientContactID);
        });
        ddlClientContactInitialise();
      })
      .catch((ex) => {
        alert("Failed to retrieve contacts." + ex);
      });
  }
};
const PopulateContactAddresses = (value: string) => {
  const ddlClientAddressID = document.getElementById(
    "ddlClientAddressID"
  ) as HTMLSelectElement;
  const selected: string = getSelected(ddlClientAddressID);
  if (ddlClientAddressID != null) {
    ddlClientAddressID.innerHTML = "";
  }
  let ContactID: number = 0;
  if (value != null) {
    if (value.toString() === "0") {
      ContactID = 0;
    } else {
      ContactID = parseInt(value);
    }
  }
  const hidGetClientContactsAddresses = document.getElementById(
    "hidGetClientContactsAddresses"
  ) as HTMLFormElement;
  fetch(
    hidGetClientContactsAddresses.value.toString() +
      "?id=" +
      encodeURIComponent(ContactID),
    {
      method: "POST",
    }
  )
    .then((response) => {
      return response.json();
    })
    .then(function (result) {
      result.forEach((elem: any) => {
        createOption(elem, selected, ddlClientAddressID);
      });
    })
    .catch((ex) => {
      alert("Failed to retrieve addresses." + ex);
    });
  setTimeout(function () {
    ddlGenericInitialise();
  }, 250);
};
const contactLoad = (value: string) => {
  PopulateContacts(parseInt(value));
};
const createOption = (elem: any, selected: string, ddl: HTMLSelectElement) => {
  const opt = document.createElement("option");
  opt.value = elem.Value;
  opt.innerHTML = elem.Text;
  if (selected === elem.Value) {
    opt.selected = true;
  }
  ddl.appendChild(opt);
};
const getSelected = (ddl: HTMLSelectElement) => {
  let selected: string = "";
  Array.from(ddl.options).forEach((element) => {
    if (element.selected) {
      selected = element.value;
    }
  });
  return selected;
};
const createContentLocation = (element: HTMLSelectElement) => {
  const loc = document.createElement("div");
  loc.id = "contentLocation" + element.id;
  loc.classList.add("contentLocation");
  element.after(loc);
};

export default ddlGenericInitialise();