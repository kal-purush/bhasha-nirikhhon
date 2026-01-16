import { clickTarget } from "@jspsych/test-utils";
import { initJsPsych, JsPsych } from "jspsych";
import Mustache from "mustache";
import videoConfig from "../templates/video-config.mustache";
import { NoStreamError } from "./errors";
import Recorder from "./recorder";

import chromeInitialPrompt from "./img/chrome_initialprompt.png";
import chromeAlwaysAllow from "./img/chrome_step1_alwaysallow.png";
import chromePermissions from "./img/chrome_step1_permissions.png";
import firefoxInitialPrompt from "./img/firefox_initialprompt.png";
import firefoxChooseDevice from "./img/firefox_prompt_choose_device.png";
import firefoxDevicesBlocked from "./img/firefox_prompt_devices_blocked.png";
import VideoConfigPlugin, { VideoConsentTrialType } from "./video_config";

jest.mock("./recorder");
jest.mock("@lookit/data");
jest.mock("jspsych", () => ({
  ...jest.requireActual("jspsych"),
  initJsPsych: jest.fn().mockReturnValue({
    finishTrial: jest.fn().mockImplementation(),
    pluginAPI: {
      initializeCameraRecorder: jest.fn(),
      getCameraStream: jest.fn().mockReturnValue({
        active: true,
        clone: jest.fn(),
        getTracks: jest.fn().mockReturnValue([{ stop: jest.fn() }]),
      }),
      getCameraRecorder: jest.fn().mockReturnValue({
        addEventListener: jest.fn(),
        start: jest.fn(),
        stop: jest.fn(),
        stream: {
          active: true,
          clone: jest.fn(),
          getTracks: jest.fn().mockReturnValue([{ stop: jest.fn() }]),
        },
      }),
    },
  }),
}));

let display_el: HTMLBodyElement;
let jsPsych: JsPsych;
let video_config: VideoConfigPlugin;
let html_params: object;
let devices: MediaDeviceInfo[];
let devicesObj: {
  cam1: MediaDeviceInfo;
  cam2: MediaDeviceInfo;
  mic1: MediaDeviceInfo;
  mic2: MediaDeviceInfo;
};
let returnedDeviceLists: {
  cameras: MediaDeviceInfo[];
  mics: MediaDeviceInfo[];
};

beforeEach(() => {
  jsPsych = initJsPsych();
  display_el = document.getElementsByTagName("body")[0] as HTMLBodyElement;
  video_config = new VideoConfigPlugin(jsPsych);
  video_config["display_el"] = display_el;

  // Set up parameters for rendering HTML template with Mustache
  html_params = {
    webcam_container_id: video_config["webcam_container_id"],
    reload_button_id_cam: video_config["reload_button_id_cam"],
    camera_selection_id: video_config["camera_selection_id"],
    mic_selection_id: video_config["mic_selection_id"],
    step1_id: video_config["step1_id"],
    step2_id: video_config["step2_id"],
    step3_id: video_config["step3_id"],
    step_complete_class: video_config["step_complete_class"],
    step_complete_text: video_config["step_complete_text"],
    reload_button_id_text: video_config["reload_button_id_text"],
    next_button_id: video_config["next_button_id"],
    chromeInitialPrompt: chromeInitialPrompt,
    chromeAlwaysAllow: chromeAlwaysAllow,
    chromePermissions: chromePermissions,
    firefoxInitialPrompt: firefoxInitialPrompt,
    firefoxChooseDevice: firefoxChooseDevice,
    firefoxDevicesBlocked: firefoxDevicesBlocked,
    troubleshooting_intro: "",
  };

  // Mocks for handling media streams and devices
  const mic1 = {
    deviceId: "mic1",
    kind: "audioinput",
    label: "",
    groupId: "default",
  } as MediaDeviceInfo;
  const cam1 = {
    deviceId: "cam1",
    kind: "videoinput",
    label: "",
    groupId: "default",
  } as MediaDeviceInfo;
  const mic2 = {
    deviceId: "mic2",
    kind: "audioinput",
    label: "",
    groupId: "other",
  } as MediaDeviceInfo;
  const cam2 = {
    deviceId: "cam2",
    kind: "videoinput",
    label: "",
    groupId: "other",
  } as MediaDeviceInfo;

  devicesObj = { cam1, cam2, mic1, mic2 };

  // Array format for devices returned from global.navigator.mediaDevices.enumerateDevices
  devices = [cam1, cam2, mic1, mic2];
  Object.defineProperty(global.navigator, "mediaDevices", {
    writable: true,
    value: {
      ondevicechange: jest.fn(),
      enumerateDevices: jest.fn(
        () =>
          new Promise<MediaDeviceInfo[]>((resolve) => {
            resolve(devices);
          }),
      ),
    },
  });

  // Object format for devices returned from getDeviceLists
  returnedDeviceLists = {
    cameras: [devicesObj.cam1, devicesObj.cam2],
    mics: [devicesObj.mic1, devicesObj.mic2],
  };
});

afterEach(() => {
  jest.clearAllMocks();
  // Reset the document body.
  document.body.innerHTML = "";
});

test("Video config trial method sets up trial", () => {
  const trial_info = {
    troubleshooting_intro: "",
  } as unknown as VideoConsentTrialType;

  // Set up mocks for upstream functions called in trial method.
  const addHtmlMock = jest
    .spyOn(video_config, "addHtmlContent")
    .mockImplementation(jest.fn());
  const addEventListenerslMock = jest
    .spyOn(video_config, "addEventListeners")
    .mockImplementation(jest.fn());
  const setupRecorderMock = jest
    .spyOn(video_config, "setupRecorder")
    .mockImplementation(jest.fn());

  expect(addHtmlMock).toHaveBeenCalledTimes(0);
  expect(addEventListenerslMock).toHaveBeenCalledTimes(0);
  expect(setupRecorderMock).toHaveBeenCalledTimes(0);
  expect(video_config["start_time"]).toBeNull();

  video_config.trial(display_el, trial_info);

  expect(global.navigator.mediaDevices.ondevicechange).toBe(
    video_config["onDeviceChange"],
  );
  expect(addHtmlMock).toHaveBeenCalledTimes(1);
  expect(addEventListenerslMock).toHaveBeenCalledTimes(1);
  expect(setupRecorderMock).toHaveBeenCalledTimes(1);
  expect(video_config["start_time"]).not.toBeNull();
  expect(video_config["start_time"]).toBeGreaterThan(0);
});

test("Video config addHtmlContent loads template", () => {
  expect(video_config["display_el"]?.innerHTML).toBe("");

  // Render the template with the HTML parameters.
  let rendered_trial_html = Mustache.render(videoConfig, html_params);
  // Remove new lines, indents (tabs or spaces), and empty HTML property values.
  rendered_trial_html = rendered_trial_html.replace(
    /(\r\n|\n|\r|\t| {4})/gm,
    "",
  );

  // Run addHtmlContent to get the actual trial HTML.
  video_config["addHtmlContent"]("");
  let displayed_html = document.body.innerHTML;
  displayed_html = displayed_html.replace(/(\r\n|\n|\r|\t| {4})/gm, "");
  displayed_html = displayed_html.replace(/(="")/gm, "");

  expect(displayed_html).toContain(rendered_trial_html);
});

test("Video config addHtmlContent loads template with custom troubleshooting text", () => {
  expect(video_config["display_el"]?.innerHTML).toBe("");

  // Render the template with a custom trial parameter.
  const troubleshooting_intro = "Custom text.";
  const html_params_custom_intro = {
    ...html_params,
    troubleshooting_intro,
  };
  let rendered_trial_html = Mustache.render(
    videoConfig,
    html_params_custom_intro,
  );
  // Remove new lines, indents (tabs or spaces), and empty HTML property values.
  rendered_trial_html = rendered_trial_html.replace(
    /(\r\n|\n|\r|\t| {4})/gm,
    "",
  );

  // Get the actual trial HTML
  video_config["addHtmlContent"](troubleshooting_intro);
  let displayed_html = document.body.innerHTML;
  displayed_html = displayed_html.replace(/(\r\n|\n|\r|\t| {4})/gm, "");
  displayed_html = displayed_html.replace(/(="")/gm, "");

  expect(displayed_html).toContain(rendered_trial_html);
});

test("Video config add event listeners", async () => {
  expect(video_config["display_el"]?.innerHTML).toBe("");
  video_config["addHtmlContent"]("");

  // Get relevant elements (device selection elements tested separately)
  const next_button_el = video_config["display_el"]?.querySelector(
    `#${video_config["next_button_id"]}`,
  ) as HTMLButtonElement;
  const reload_button_els = video_config["display_el"]?.querySelectorAll(
    `#${video_config["reload_button_id_cam"]}, #${video_config["reload_button_id_text"]}`,
  ) as NodeListOf<HTMLButtonElement>;
  const acc_button_els = document.getElementsByClassName(
    "lookit-jspsych-accordion",
  ) as HTMLCollectionOf<HTMLButtonElement>;

  // Mock event-related callback functions.
  const nextBtnClickMock = jest
    .spyOn(video_config, "nextButtonClick")
    .mockImplementation(jest.fn());
  const reloadBtnClickMock = jest
    .spyOn(video_config, "reloadButtonClick")
    .mockImplementation(jest.fn());

  video_config["addEventListeners"]();

  // Next button is disabled when the page loads - it must be enabled before it will accept a click event.
  video_config["enableNext"](true);
  expect(nextBtnClickMock).toHaveBeenCalledTimes(0);
  await clickTarget(next_button_el);
  expect(nextBtnClickMock).toHaveBeenCalledTimes(1);

  // Reload buttons.
  expect(reloadBtnClickMock.mock.calls).toHaveLength(0);
  for (let i = 0; i < reload_button_els.length; i++) {
    await clickTarget(reload_button_els[i]);
  }
  expect(reloadBtnClickMock.mock.calls).toHaveLength(reload_button_els.length);

  // Buttons and content (panel) for accordion section (troubleshooting)
  for (let i = 0; i < acc_button_els.length; i++) {
    const this_btn = acc_button_els[i];
    const this_panel = this_btn.nextElementSibling as HTMLDivElement;

    // Initial state is no active class and panel display: none
    expect(this_btn.classList.contains("active")).toBe(false);
    expect(this_panel.style.display).toBe("none");

    await clickTarget(this_btn);

    // Button active and panel display: block
    expect(this_btn.classList.contains("active")).toBe(true);
    expect(this_panel.style.display).toBe("block");

    await clickTarget(this_btn);

    // Clicking it again returns it to initial state.
    expect(this_btn.classList.contains("active")).toBe(false);
    expect(this_panel.style.display).toBe("none");
  }
});

test("Video config enable next", () => {
  expect(video_config["display_el"]?.innerHTML).toBe("");

  video_config["addHtmlContent"]("");
  const next_button_el = video_config["display_el"]?.querySelector(
    `#${video_config["next_button_id"]}`,
  ) as HTMLButtonElement;

  // When trial first loads, next button should exist but be disabled and no 'step_complete_class'
  expect(next_button_el).toBeTruthy();
  expect(
    next_button_el.classList.contains(video_config["step_complete_class"]),
  ).toBe(false);
  expect(next_button_el.disabled).toBe(true);

  // Calling with 'true' enables the button and adds the class.
  video_config["enableNext"](true);
  expect(
    next_button_el.classList.contains(video_config["step_complete_class"]),
  ).toBe(true);
  expect(next_button_el.disabled).toBe(false);

  // Calling with 'false' sets everything back.
  video_config["enableNext"](false);
  expect(
    next_button_el.classList.contains(video_config["step_complete_class"]),
  ).toBe(false);
  expect(next_button_el.disabled).toBe(true);
});

test("Video config update instructions", () => {
  expect(video_config["display_el"]?.innerHTML).toBe("");
  video_config["addHtmlContent"]("");

  // Get the relevant elements from the instructions section.
  const step1_id = video_config["step1_id"];
  const step2_id = video_config["step2_id"];
  const step3_id = video_config["step3_id"];
  const step1_span = video_config["display_el"]?.querySelector(
    `#${step1_id}-span`,
  ) as HTMLSpanElement;
  const step1_para = video_config["display_el"]?.querySelector(
    `#${step1_id}-paragraph`,
  ) as HTMLParagraphElement;
  const step2_span = video_config["display_el"]?.querySelector(
    `#${step2_id}-span`,
  ) as HTMLSpanElement;
  const step2_para = video_config["display_el"]?.querySelector(
    `#${step2_id}-paragraph`,
  ) as HTMLParagraphElement;
  const step3_span = video_config["display_el"]?.querySelector(
    `#${step3_id}-span`,
  ) as HTMLSpanElement;
  const step3_para = video_config["display_el"]?.querySelector(
    `#${step3_id}-paragraph`,
  ) as HTMLParagraphElement;

  // For each step 1-3:
  // - calling with 'true' should make the span element visible and set the paragraph text color to gray.
  // - calling with 'false' should make the span hidden and set the paragraph text color to black.
  // The initial state is the same as with 'false', except that the text color is unset (inherits).
  expect(step1_span.style.visibility).toBe("hidden");
  video_config["updateInstructions"](1, true);
  expect(step1_span.style.visibility).toBe("visible");
  expect(step1_para.style.color).toBe("gray");
  video_config["updateInstructions"](1, false);
  expect(step1_span.style.visibility).toBe("hidden");
  expect(step1_para.style.color).toBe("black");

  expect(step2_span.style.visibility).toBe("hidden");
  video_config["updateInstructions"](2, true);
  expect(step2_span.style.visibility).toBe("visible");
  expect(step2_para.style.color).toBe("gray");
  video_config["updateInstructions"](2, false);
  expect(step2_span.style.visibility).toBe("hidden");
  expect(step2_para.style.color).toBe("black");

  expect(step3_span.style.visibility).toBe("hidden");
  video_config["updateInstructions"](3, true);
  expect(step3_span.style.visibility).toBe("visible");
  expect(step3_para.style.color).toBe("gray");
  video_config["updateInstructions"](3, false);
  expect(step3_span.style.visibility).toBe("hidden");
  expect(step3_para.style.color).toBe("black");
});

test("Video config update errors", () => {
  expect(video_config["display_el"]?.innerHTML).toBe("");
  video_config["addHtmlContent"]("");
  const error_msg_div = video_config["display_el"]?.querySelector(
    `#${video_config["error_msg_div_id"]}`,
  ) as HTMLDivElement;

  // Error/info message div is empty when the page first loads.
  expect(error_msg_div.innerHTML).toStrictEqual("");

  const test_msg = "Test message.";
  video_config["updateErrors"](test_msg);

  expect(error_msg_div.innerHTML).toStrictEqual(test_msg);
});

test("Video config reload button click", async () => {
  expect(video_config["hasReloaded"]).toBe(false);

  video_config["addHtmlContent"]("");

  const reload_button_els = video_config["display_el"]?.querySelectorAll(
    `#${video_config["reload_button_id_cam"]}, #${video_config["reload_button_id_text"]}`,
  ) as NodeListOf<HTMLButtonElement>;

  // Mock upstream function calls.
  const updateInstructionskMock = jest
    .spyOn(video_config, "updateInstructions")
    .mockImplementation(jest.fn());
  const destroyMock = jest
    .spyOn(video_config, "destroyRecorder")
    .mockImplementation(jest.fn());
  const setupRecorderMock = jest
    .spyOn(video_config, "setupRecorder")
    .mockImplementation(jest.fn());

  video_config["addEventListeners"]();

  expect(updateInstructionskMock).toHaveBeenCalledTimes(0);
  expect(destroyMock).toHaveBeenCalledTimes(0);
  expect(setupRecorderMock).toHaveBeenCalledTimes(0);

  await clickTarget(reload_button_els[0]);

  expect(updateInstructionskMock).toHaveBeenCalledTimes(1);
  expect(destroyMock).toHaveBeenCalledTimes(1);
  expect(setupRecorderMock).toHaveBeenCalledTimes(1);
  expect(video_config["hasReloaded"]).toBe(true);

  await clickTarget(reload_button_els[1]);

  expect(updateInstructionskMock).toHaveBeenCalledTimes(2);
  expect(destroyMock).toHaveBeenCalledTimes(2);
  expect(setupRecorderMock).toHaveBeenCalledTimes(2);
  expect(video_config["hasReloaded"]).toBe(true);
});

test("Video config updateDeviceSelection", () => {
  video_config["addHtmlContent"]("");
  const cam_selection_el = video_config["display_el"]?.querySelector(
    `#${video_config["camera_selection_id"]}`,
  ) as HTMLSelectElement;
  const mic_selection_el = video_config["display_el"]?.querySelector(
    `#${video_config["mic_selection_id"]}`,
  ) as HTMLSelectElement;

  expect(cam_selection_el).not.toBeUndefined();
  expect(cam_selection_el.innerHTML).toBe("");
  expect(mic_selection_el).not.toBeUndefined();
  expect(mic_selection_el.innerHTML).toBe("");

  // Populates the select elements with new device info.
  video_config["updateDeviceSelection"]({
    cameras: [devicesObj.cam1],
    mics: [devicesObj.mic1],
  });
  expect(mic_selection_el.innerHTML).toContain(
    `<option value="${devicesObj.mic1.deviceId}"></option>`,
  );
  expect(cam_selection_el.innerHTML).toContain(
    `<option value="${devicesObj.cam1.deviceId}"></option>`,
  );

  // Also clears old options in select elements that are no longer in the devices lists.
  video_config["updateDeviceSelection"]({
    cameras: [devicesObj.cam2],
    mics: [devicesObj.mic2],
  });
  expect(mic_selection_el.innerHTML).not.toContain(
    `<option value="${devicesObj.mic1.deviceId}"></option>`,
  );
  expect(cam_selection_el.innerHTML).not.toContain(
    `<option value="${devicesObj.cam1.deviceId}"></option>`,
  );
  expect(mic_selection_el.innerHTML).toContain(
    `<option value="${devicesObj.mic2.deviceId}"></option>`,
  );
  expect(cam_selection_el.innerHTML).toContain(
    `<option value="${devicesObj.cam2.deviceId}"></option>`,
  );

  // If a device was previously selected (via setDevices), and is in the updated list, it is automatically selected again.
  video_config["camId"] = devicesObj.cam2.deviceId;
  video_config["micId"] = devicesObj.mic2.deviceId;
  cam_selection_el.value = devicesObj.cam2.deviceId;
  mic_selection_el.value = devicesObj.mic2.deviceId;
  video_config["updateDeviceSelection"]({
    cameras: [devicesObj.cam1, devicesObj.cam2],
    mics: [devicesObj.mic1, devicesObj.mic2],
  });
  expect(mic_selection_el.innerHTML).toContain(
    `<option value="${devicesObj.mic1.deviceId}"></option>`,
  );
  expect(cam_selection_el.innerHTML).toContain(
    `<option value="${devicesObj.cam1.deviceId}"></option>`,
  );
  expect(mic_selection_el.innerHTML).toContain(
    `<option value="${devicesObj.mic2.deviceId}"></option>`,
  );
  expect(cam_selection_el.innerHTML).toContain(
    `<option value="${devicesObj.cam2.deviceId}"></option>`,
  );
  expect(cam_selection_el.value).toBe(devicesObj.cam2.deviceId);
  expect(mic_selection_el.value).toBe(devicesObj.mic2.deviceId);

  // If a previously-selected device is no longer in the list, there is no error.
  video_config["updateDeviceSelection"]({
    cameras: [devicesObj.cam1],
    mics: [devicesObj.mic1],
  });
  expect(mic_selection_el.innerHTML).toContain(
    `<option value="${devicesObj.mic1.deviceId}"></option>`,
  );
  expect(cam_selection_el.innerHTML).toContain(
    `<option value="${devicesObj.cam1.deviceId}"></option>`,
  );
  expect(mic_selection_el.innerHTML).not.toContain(
    `<option value="${devicesObj.mic2.deviceId}"></option>`,
  );
  expect(cam_selection_el.innerHTML).not.toContain(
    `<option value="${devicesObj.cam2.deviceId}"></option>`,
  );
  expect(cam_selection_el.value).toBe(devicesObj.cam1.deviceId);
  expect(mic_selection_el.value).toBe(devicesObj.mic1.deviceId);
});

test("Video config end trial", () => {
  // Set up some data values
  const response = { rt: 1000 };
  const camId = "cam Id";
  const micId = "mic Id";
  video_config["response"] = response;
  video_config["camId"] = camId;
  video_config["micId"] = micId;

  // Ending the trial should trigger finishTrial with the data.
  video_config["endTrial"]();
  expect(jsPsych.finishTrial).toHaveBeenCalledWith({
    rt: response.rt,
    camId,
    micId,
  });
});

test("Video config setupRecorder", async () => {
  // Mock the upstream function calls.
  const updateInstructionsMock = jest
    .spyOn(video_config, "updateInstructions")
    .mockImplementation(jest.fn());
  const updateErrorsMock = jest
    .spyOn(video_config, "updateErrors")
    .mockImplementation(jest.fn());
  const requestPermissionMock = jest
    .spyOn(video_config, "requestPermission")
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    .mockImplementation((constraints: MediaStreamConstraints) =>
      Promise.resolve(jsPsych.pluginAPI.getCameraRecorder().stream),
    );
  const getDeviceListsMock = jest
    .spyOn(video_config, "getDeviceLists")
    .mockImplementation(
      (
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        include_audio?: boolean | undefined,
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        include_camera?: boolean | undefined,
      ) => {
        return Promise.resolve(returnedDeviceLists);
      },
    );
  const updateDeviceSelectionMock = jest
    .spyOn(video_config, "updateDeviceSelection")
    .mockImplementation(jest.fn());
  const setDevicesMock = jest
    .spyOn(video_config, "setDevices")
    .mockImplementation(jest.fn());
  const runStreamChecksMock = jest
    .spyOn(video_config, "runStreamChecks")
    .mockImplementation(jest.fn());

  await video_config["setupRecorder"]();

  // Updates the Instructions - resets the "completed" status of steps 1 and 3.
  expect(updateInstructionsMock).toHaveBeenCalledTimes(2);
  expect(updateInstructionsMock.mock.calls[0]).toStrictEqual([1, false]);
  expect(updateInstructionsMock.mock.calls[1]).toStrictEqual([3, false]);
  // Adds error message when waiting for stream access, then clears the message after access is granted.
  expect(updateErrorsMock).toHaveBeenCalledTimes(2);
  expect(updateErrorsMock.mock.calls[0]).toStrictEqual([
    video_config["waiting_for_access_msg"],
  ]);
  expect(updateErrorsMock.mock.calls[1]).toStrictEqual([""]);
  // Needs to request permissions before it can get device lists.
  expect(requestPermissionMock).toHaveBeenCalledWith({
    video: true,
    audio: true,
  });
  // Gets device lists.
  expect(getDeviceListsMock).toHaveBeenCalledTimes(1);
  // Updates selection elements with available devices.
  expect(updateDeviceSelectionMock).toHaveBeenCalledTimes(1);
  expect(updateDeviceSelectionMock).toHaveBeenCalledWith(returnedDeviceLists);
  // Set the current devices.
  expect(setDevicesMock).toHaveBeenCalledTimes(1);
  // Run stream checks on current devices.
  expect(runStreamChecksMock).toHaveBeenCalledTimes(1);
});

test("Video config setDevices", async () => {
  video_config["addHtmlContent"]("");
  const cam_selection_el = video_config["display_el"]?.querySelector(
    `#${video_config["camera_selection_id"]}`,
  ) as HTMLSelectElement;
  const mic_selection_el = video_config["display_el"]?.querySelector(
    `#${video_config["mic_selection_id"]}`,
  ) as HTMLSelectElement;
  const next_button_el = video_config["display_el"]?.querySelector(
    `#${video_config["next_button_id"]}`,
  ) as HTMLButtonElement;
  video_config["recorder"] = new Recorder(jsPsych);

  // Populate the selection elements with device options.
  video_config["updateDeviceSelection"]({
    cameras: [devicesObj.cam1, devicesObj.cam2],
    mics: [devicesObj.mic1, devicesObj.mic2],
  });

  // Select some devices.
  cam_selection_el.value = devicesObj.cam2.deviceId;
  mic_selection_el.value = devicesObj.mic2.deviceId;

  // Setup upstream mocks.
  const enableNextMock = jest
    .spyOn(video_config, "enableNext")
    .mockImplementation(jest.fn());
  const updateInstructionsMock = jest
    .spyOn(video_config, "updateInstructions")
    .mockImplementation(jest.fn());
  const requestPermissionMock = jest
    .spyOn(video_config, "requestPermission")
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    .mockImplementation((constraints: MediaStreamConstraints) =>
      Promise.resolve(jsPsych.pluginAPI.getCameraRecorder().stream),
    );
  const createInitializeRecorderMock = jest.spyOn(
    video_config,
    "initializeAndCreateRecorder",
  );

  expect(next_button_el.disabled).toBe(true);
  expect(video_config["camId"]).toStrictEqual("");
  expect(video_config["micId"]).toStrictEqual("");

  await video_config["setDevices"]();

  // Disables next button
  expect(enableNextMock).toHaveBeenCalledTimes(1);
  expect(enableNextMock).toHaveBeenCalledWith(false);
  expect(next_button_el.disabled).toBe(true);
  // Updates instructions step 3 (mic check)
  expect(updateInstructionsMock).toHaveBeenCalledTimes(1);
  expect(updateInstructionsMock).toHaveBeenCalledWith(3, false);
  // Sets this.camId and this.micId values based on selection elements.
  expect(video_config["camId"]).toStrictEqual(devicesObj.cam2.deviceId);
  expect(video_config["micId"]).toStrictEqual(devicesObj.mic2.deviceId);
  // Requests permissions for those specific devices
  expect(requestPermissionMock).toHaveBeenCalledWith({
    video: { deviceId: devicesObj.cam2.deviceId },
    audio: { deviceId: devicesObj.mic2.deviceId },
  });
  // Calls initializeAndCreateRecorder with the returned stream
  expect(createInitializeRecorderMock).toHaveBeenCalledWith(
    jsPsych.pluginAPI.getCameraRecorder().stream,
  );

  // Now change the device selections - values should update accordingly.
  cam_selection_el.value = devicesObj.cam1.deviceId;
  mic_selection_el.value = devicesObj.mic1.deviceId;

  await video_config["setDevices"]();
  expect(video_config["camId"]).toStrictEqual(devicesObj.cam1.deviceId);
  expect(video_config["micId"]).toStrictEqual(devicesObj.mic1.deviceId);
  expect(requestPermissionMock).toHaveBeenCalledWith({
    video: { deviceId: devicesObj.cam1.deviceId },
    audio: { deviceId: devicesObj.mic1.deviceId },
  });
});

test("Video config runStreamChecks throws NoStreamAccess", () => {
  video_config["addHtmlContent"]("");
  video_config["recorder"] = new Recorder(initJsPsych());
  jest
    .spyOn(jsPsych.pluginAPI, "getCameraRecorder")
    // @ts-expect-error - jsPsych says the return value should be MediaRecorder, but in fact it's MediaRecorder or null.
    .mockImplementationOnce(() => null);
  // When the recorder has not been initialized with jsPsych.
  expect(async () => await video_config["runStreamChecks"]()).rejects.toThrow(
    NoStreamError,
  );

  expect(jsPsych.pluginAPI.getCameraRecorder).toBeTruthy();

  // When the Recorder has not been created.
  video_config["recorder"] = null;
  expect(async () => await video_config["runStreamChecks"]()).rejects.toThrow(
    NoStreamError,
  );
});

test("Video config runStreamChecks throws Mic Check error", async () => {
  video_config["addHtmlContent"]("");
  video_config["recorder"] = new Recorder(jsPsych);

  // Setup upstream mocks.
  Recorder.prototype.insertWebcamFeed = jest.fn();
  video_config["updateInstructions"] = jest.fn();
  const updateErrorsMock = jest
    .spyOn(video_config, "updateErrors")
    .mockImplementation(jest.fn());
  // Mock an error during the mic check setup.
  const mockError = jest.fn(() => {
    const promise = new Promise<void>(() => {
      throw "Mic check problem";
    });
    promise.catch(() => null); // Prevent an uncaught error here so that it propogates to the catch block.
    return promise;
  });
  video_config["checkMic"] = mockError;

  await expect(video_config["runStreamChecks"]()).rejects.toThrow(
    "Mic check problem",
  );

  // If the recorder mic check throws an error, then the plugin should update the error message and throw the error.
  expect(updateErrorsMock).toHaveBeenCalledTimes(2);
  expect(updateErrorsMock.mock.calls[0]).toStrictEqual([
    video_config["checking_mic_msg"],
  ]);
  expect(updateErrorsMock.mock.calls[1]).toStrictEqual([
    video_config["setup_problem_msg"],
  ]);
});

test("Video config runStreamChecks", async () => {
  video_config["addHtmlContent"]("");
  video_config["recorder"] = new Recorder(jsPsych);

  // Setup upstream mocks.
  const insertWebcamMock = jest
    .spyOn(Recorder.prototype, "insertWebcamFeed")
    .mockImplementation(jest.fn());
  const updateInstructionsMock = jest
    .spyOn(video_config, "updateInstructions")
    .mockImplementation(jest.fn());
  const updateErrorsMock = jest
    .spyOn(video_config, "updateErrors")
    .mockImplementation(jest.fn());
  const checkMicMock = jest
    .spyOn(video_config, "checkMic")
    .mockImplementation(jest.fn(() => Promise.resolve()));

  await video_config["runStreamChecks"]();

  expect(insertWebcamMock).toHaveBeenCalledTimes(1);
  // Called twice: first for passing the stream access check, then for passing the mic input check.
  expect(updateInstructionsMock).toHaveBeenCalledTimes(2);
  expect(updateInstructionsMock.mock.calls[0]).toStrictEqual([1, true]);
  expect(updateInstructionsMock.mock.calls[1]).toStrictEqual([3, true]);
  // Called twice: first with "checking mic" message, then to clear that message.
  expect(updateErrorsMock).toHaveBeenCalledTimes(2);
  expect(updateErrorsMock.mock.calls[0]).toStrictEqual([
    video_config["checking_mic_msg"],
  ]);
  expect(updateErrorsMock.mock.calls[1]).toStrictEqual([""]);
  expect(checkMicMock).toHaveBeenCalledTimes(1);
});

test("Video config runStreamChecks enables next button when all checks have passed", async () => {
  video_config["addHtmlContent"]("");
  video_config["recorder"] = new Recorder(jsPsych);

  // Setup upstream mocks.
  Recorder.prototype.insertWebcamFeed = jest.fn();
  video_config["updateInstructions"] = jest.fn();
  video_config["updateErrors"] = jest.fn();
  video_config["checkMic"] = jest.fn(() => Promise.resolve());
  const enableNextMock = jest
    .spyOn(video_config, "enableNext")
    .mockImplementation(jest.fn());

  video_config["hasReloaded"] = true;

  await video_config["runStreamChecks"]();

  expect(enableNextMock).toHaveBeenCalledTimes(1);
  expect(enableNextMock).toHaveBeenCalledWith(true);
});

test("Video config onDeviceChange event listener", async () => {
  video_config["recorder"] = new Recorder(jsPsych);
  // Mock the MediaDevices API
  const mockMediaDevices = {
    ondevicechange: null as ((this: MediaDevices, ev: Event) => void) | null,
  };
  global.navigator["mediaDevices"] = mockMediaDevices;

  // The onDeviceChange event handler is set up in the plugin's trial method.
  // Set up mocks for all other functions called in trial method.
  video_config["addHtmlContent"] = jest.fn();
  video_config["addEventListeners"] = jest.fn();
  video_config["setupRecorder"] = jest.fn();

  // Setup upstream mocks for functions called inside onDeviceChange.
  const getDeviceListsMock = jest
    .spyOn(video_config, "getDeviceLists")
    .mockImplementation(
      (
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        include_audio?: boolean | undefined,
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        include_camera?: boolean | undefined,
      ) => {
        return Promise.resolve(returnedDeviceLists);
      },
    );
  const updateDeviceSelectionMock = jest
    .spyOn(video_config, "updateDeviceSelection")
    .mockImplementation(jest.fn());

  // Call trial method to set up onDeviceChange.
  expect(navigator.mediaDevices.ondevicechange).toBeNull();
  const trial_info = {
    troubleshooting_intro: "",
  } as unknown as VideoConsentTrialType;
  video_config.trial(display_el, trial_info);
  expect(navigator.mediaDevices.ondevicechange).not.toBeNull();

  // Simulate a device change event.
  const event = new Event("devicechange");
  await navigator.mediaDevices.ondevicechange?.(event);

  expect(getDeviceListsMock).toHaveBeenCalledTimes(1);
  expect(updateDeviceSelectionMock).toHaveBeenCalledWith(returnedDeviceLists);
});

test("Video config device selection element on change event listener", async () => {
  video_config["addHtmlContent"]("");

  // Mock upstream functions called by device selection change event listener.
  const setDevicesMock = jest
    .spyOn(video_config, "setDevices")
    .mockImplementation(jest.fn(() => Promise.resolve()));
  const runStreamChecksMock = jest
    .spyOn(video_config, "runStreamChecks")
    .mockImplementation(jest.fn(() => Promise.resolve()));

  // Get device selection elements.
  const device_selection_els = video_config["display_el"]?.querySelectorAll(
    ".lookit-jspsych-device-selection",
  ) as NodeListOf<HTMLSelectElement>;

  video_config["addEventListeners"]();

  expect(setDevicesMock.mock.calls).toHaveLength(0);
  expect(runStreamChecksMock.mock.calls).toHaveLength(0);

  // Simulate device selection change events.
  for (let i = 0; i < device_selection_els.length; i++) {
    const changeEvent = new Event("change", { bubbles: true });
    await device_selection_els[i].dispatchEvent(changeEvent);
  }

  expect(setDevicesMock.mock.calls).toHaveLength(device_selection_els.length);
  expect(runStreamChecksMock.mock.calls).toHaveLength(
    device_selection_els.length,
  );
});

test("Video config next button click", async () => {
  video_config["start_time"] = 1;
  const perfNowMock = jest
    .spyOn(performance, "now")
    .mockImplementation(jest.fn().mockReturnValue(5));
  const destroyMock = jest
    .spyOn(video_config, "destroyRecorder")
    .mockImplementation(jest.fn().mockReturnValue(Promise.resolve()));
  const endMock = jest
    .spyOn(video_config, "endTrial")
    .mockImplementation(jest.fn());

  await video_config["nextButtonClick"]();

  expect(perfNowMock).toHaveBeenCalledTimes(1);
  expect(video_config["response"]).toHaveProperty("rt");
  expect(video_config["response"]["rt"]).toBe(4);
  expect(destroyMock).toHaveBeenCalledTimes(1);
  expect(endMock.mock.calls).toHaveLength(1);

  video_config["start_time"] = null;

  await video_config["nextButtonClick"]();

  expect(video_config["response"]).toHaveProperty("rt");
  expect(video_config["response"]["rt"]).toBeNull();
});

test("Video config destroyRecorder", () => {
  const recorderStopTracksSpy = jest.spyOn(Recorder.prototype, "stopTracks");
  video_config["recorder"] = new Recorder(jsPsych);
  const enableNextMock = jest
    .spyOn(video_config, "enableNext")
    .mockImplementation(jest.fn());
  const updateInstructionsMock = jest
    .spyOn(video_config, "updateInstructions")
    .mockImplementation(jest.fn());

  expect(video_config["recorder"]).not.toBeNull();

  video_config["destroyRecorder"]();

  // Should stop the tracks, set the recorder to null, disable the next button,
  // and update the instructions (checks 1/3 not passed).
  expect(recorderStopTracksSpy).toHaveBeenCalledTimes(1);
  expect(video_config["recorder"]).toBeNull();
  expect(enableNextMock).toHaveBeenCalledTimes(1);
  expect(enableNextMock).toHaveBeenCalledWith(false);
  expect(updateInstructionsMock).toHaveBeenCalledTimes(2);
  expect(updateInstructionsMock.mock.calls[0]).toStrictEqual([3, false]);
  expect(updateInstructionsMock.mock.calls[1]).toStrictEqual([1, false]);
});

test("Video config destroyRecorder with no recorder", () => {
  const recorderStopTracksSpy = jest.spyOn(Recorder.prototype, "stopTracks");
  const enableNextMock = jest
    .spyOn(video_config, "enableNext")
    .mockImplementation(jest.fn());
  const updateInstructionsMock = jest
    .spyOn(video_config, "updateInstructions")
    .mockImplementation(jest.fn());

  expect(video_config["recorder"]).toBeNull();

  video_config["destroyRecorder"]();

  expect(recorderStopTracksSpy).not.toHaveBeenCalled();
  expect(video_config["recorder"]).toBeNull();
  expect(enableNextMock).toHaveBeenCalledTimes(1);
  expect(enableNextMock).toHaveBeenCalledWith(false);
  expect(updateInstructionsMock).toHaveBeenCalledTimes(2);
  expect(updateInstructionsMock.mock.calls[0]).toStrictEqual([3, false]);
  expect(updateInstructionsMock.mock.calls[1]).toStrictEqual([1, false]);
});

test("Video config requestPermission", async () => {
  const stream = { fake: "stream" } as unknown as MediaStream;
  const mockGetUserMedia = jest.fn(
    () =>
      new Promise<MediaStream>((resolve) => {
        resolve(stream);
      }),
  );
  Object.defineProperty(global.navigator, "mediaDevices", {
    writable: true,
    value: {
      getUserMedia: mockGetUserMedia,
    },
  });
  const constraints = { video: true, audio: true };

  const returnedStream = await video_config.requestPermission(constraints);
  expect(returnedStream).toStrictEqual(stream);
  expect(global.navigator.mediaDevices.getUserMedia).toHaveBeenCalledWith(
    constraints,
  );
});

test("Video config getDeviceLists", async () => {
  const mic1 = {
    deviceId: "mic1",
    kind: "audioinput",
    label: "",
    groupId: "default",
  } as MediaDeviceInfo;
  const cam1 = {
    deviceId: "cam1",
    kind: "videoinput",
    label: "",
    groupId: "default",
  } as MediaDeviceInfo;
  const mic2 = {
    deviceId: "mic2",
    kind: "audioinput",
    label: "",
    groupId: "other",
  } as MediaDeviceInfo;
  const cam2 = {
    deviceId: "cam2",
    kind: "videoinput",
    label: "",
    groupId: "other",
  } as MediaDeviceInfo;

  // Returns the mic/cam devices from navigator.mediaDevices.enumerateDevices as an object with 'cameras' and 'mics' (arrays of media device info objects).
  const devices = [mic1, mic2, cam1, cam2];
  Object.defineProperty(global.navigator, "mediaDevices", {
    writable: true,
    value: {
      enumerateDevices: jest.fn(
        () =>
          new Promise<MediaDeviceInfo[]>((resolve) => {
            resolve(devices);
          }),
      ),
    },
  });

  const returnedDevices = await video_config.getDeviceLists();
  expect(global.navigator.mediaDevices.enumerateDevices).toHaveBeenCalledTimes(
    1,
  );
  expect(returnedDevices).toHaveProperty("cameras");
  expect(returnedDevices).toHaveProperty("mics");
  expect(returnedDevices.cameras.sort()).toStrictEqual([cam1, cam2].sort());
  expect(returnedDevices.mics.sort()).toStrictEqual([mic1, mic2].sort());

  // Removes duplicate devices and handles empty device categories.
  const devices_duplicate = [mic1, mic1, mic1];
  Object.defineProperty(global.navigator, "mediaDevices", {
    writable: true,
    value: {
      enumerateDevices: jest.fn(
        () =>
          new Promise<MediaDeviceInfo[]>((resolve) => {
            resolve(devices_duplicate);
          }),
      ),
    },
  });

  const returnedDevicesDuplicates = await video_config.getDeviceLists();
  expect(returnedDevicesDuplicates.cameras).toStrictEqual([]);
  expect(returnedDevicesDuplicates.mics).toStrictEqual([mic1]);
});

test("Video config onMicActivityLevel", () => {
  type micEventType = {
    currentActivityLevel: number;
    minVolume: number;
    resolve: () => void;
  };
  const event_fail = {
    currentActivityLevel: 0.0001,
    minVolume: video_config["minVolume"],
    resolve: jest.fn(),
  } as micEventType;

  expect(video_config["micChecked"]).toBe(false);
  video_config["onMicActivityLevel"](
    event_fail.currentActivityLevel,
    event_fail.minVolume,
    event_fail.resolve,
  );
  expect(video_config["micChecked"]).toBe(false);
  expect(event_fail.resolve).not.toHaveBeenCalled();

  const event_pass = {
    currentActivityLevel: 0.2,
    minVolume: video_config["minVolume"],
    resolve: jest.fn(),
  } as micEventType;

  expect(video_config["micChecked"]).toBe(false);
  video_config["onMicActivityLevel"](
    event_pass.currentActivityLevel,
    event_pass.minVolume,
    event_pass.resolve,
  );
  expect(video_config["micChecked"]).toBe(true);
  expect(event_pass.resolve).toHaveBeenCalled();
});

test("Video config onMicActivityLevel", () => {
  type micEventType = {
    currentActivityLevel: number;
    minVolume: number;
    resolve: () => void;
  };
  const event_fail = {
    currentActivityLevel: 0.0001,
    minVolume: video_config["minVolume"],
    resolve: jest.fn(),
  } as micEventType;

  expect(video_config["micChecked"]).toBe(false);
  video_config["onMicActivityLevel"](
    event_fail.currentActivityLevel,
    event_fail.minVolume,
    event_fail.resolve,
  );
  expect(video_config["micChecked"]).toBe(false);
  expect(event_fail.resolve).not.toHaveBeenCalled();

  const event_pass = {
    currentActivityLevel: 0.2,
    minVolume: video_config["minVolume"],
    resolve: jest.fn(),
  } as micEventType;

  expect(video_config["micChecked"]).toBe(false);
  video_config["onMicActivityLevel"](
    event_pass.currentActivityLevel,
    event_pass.minVolume,
    event_pass.resolve,
  );
  expect(video_config["micChecked"]).toBe(true);
  expect(event_pass.resolve).toHaveBeenCalled();
});