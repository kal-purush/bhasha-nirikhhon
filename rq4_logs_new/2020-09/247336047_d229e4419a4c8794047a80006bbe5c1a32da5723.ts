/*
 *   This content is licensed according to the W3C Software License at
 *   https://www.w3.org/Consortium/Legal/2015/copyright-software-and-document
 */
var compliantDialog = compliantDialog || {};
compliantDialog.Utils = compliantDialog.Utils || {};

(function () {
  /*
   * When util functions move focus around, set this true so the focus listener
   * can ignore the events.
   */
  compliantDialog.Utils.IgnoreUtilFocusChanges = false;

  compliantDialog.Utils.dialogOpenClass = "has-dialog";

  /**
   * @desc Set focus on descendant nodes until the first focusable element is
   *       found.
   * @param element
   *          DOM node for which to find the first focusable descendant.
   * @returns
   *  true if a focusable element is found and focus is set.
   */
  compliantDialog.Utils.focusFirstDescendant = function (element) {
    for (var i = 0; i < element.childNodes.length; i++) {
      var child = element.childNodes[i];
      if (
        compliantDialog.Utils.attemptFocus(child) ||
        compliantDialog.Utils.focusFirstDescendant(child)
      ) {
        return true;
      }
    }
    return false;
  }; // end focusFirstDescendant

  /**
   * @desc Find the last descendant node that is focusable.
   * @param element
   *          DOM node for which to find the last focusable descendant.
   * @returns
   *  true if a focusable element is found and focus is set.
   */
  compliantDialog.Utils.focusLastDescendant = function (element) {
    for (var i = element.childNodes.length - 1; i >= 0; i--) {
      var child = element.childNodes[i];
      if (
        compliantDialog.Utils.attemptFocus(child) ||
        compliantDialog.Utils.focusLastDescendant(child)
      ) {
        return true;
      }
    }
    return false;
  }; // end focusLastDescendant

  /**
   * @desc Set Attempt to set focus on the current node.
   * @param element
   *          The node to attempt to focus on.
   * @returns
   *  true if element is focused.
   */
  compliantDialog.Utils.attemptFocus = function (element) {
    // if (!compliantDialog.Utils.isFocusable(element)) {
    //   return false;
    // }

    compliantDialog.Utils.IgnoreUtilFocusChanges = true;
    try {
      element.focus();
    } catch (e) {}
    compliantDialog.Utils.IgnoreUtilFocusChanges = false;
    return document.activeElement === element;
  }; // end attemptFocus

  /* Modals can open modals. Keep track of them with this array. */
  compliantDialog.OpenDialogList =
    compliantDialog.OpenDialogList || new Array(0);

  /**
   * @returns the last opened dialog (the current dialog)
   */
  compliantDialog.getCurrentDialog = function () {
    if (
      compliantDialog.OpenDialogList &&
      compliantDialog.OpenDialogList.length
    ) {
      return compliantDialog.OpenDialogList[
        compliantDialog.OpenDialogList.length - 1
      ];
    }
  };

  compliantDialog.closeCurrentDialog = function () {
    const currentDialog = compliantDialog.getCurrentDialog();
    if (currentDialog) {
      currentDialog.close();
      return true;
    }

    return false;
  };

  /**
   * @constructor
   * @desc Dialog object providing modal focus management.
   *
   * Assumptions: The element serving as the dialog container is present in the
   * DOM and hidden. The dialog container has role='dialog'.
   *
   * @param dialogId
   *          The ID of the element serving as the dialog container.
   * @param focusAfterClosed
   *          Either the DOM node or the ID of the DOM node to focus when the
   *          dialog closes.
   * @param focusFirst
   *          Optional parameter containing either the DOM node or the ID of the
   *          DOM node to focus when the dialog opens. If not specified, the
   *          first focusable element in the dialog will receive focus.
   */
  compliantDialog.Dialog = function (dialogId, focusAfterClosed, focusFirst) {
    this.dialogNode = document.getElementById(dialogId);
    if (this.dialogNode === null) {
      return;
    }

    const validRole = "dialog";
    const isDialog = (this.dialogNode.getAttribute("role") || "")
      .trim()
      .split(/\s+/g)
      .some(function (token) {
        return token === validRole;
        // return validRoles.some(function (role) {
        //   return token === role;
        // });
      });
    if (!isDialog) {
      return;
    }

    // Wrap in an individual backdrop element if one doesn't exist
    // Native <dialog> elements use the ::backdrop pseudo-element, which
    // works similarly.
    const backdropClass = "dialog-backdrop";
    if (this.dialogNode.parentNode.classList.contains(backdropClass)) {
      this.backdropNode = this.dialogNode.parentNode;
    } else {
      this.backdropNode = document.createElement("div");
      this.backdropNode.className = backdropClass;
      this.dialogNode.parentNode.insertBefore(
        this.backdropNode,
        this.dialogNode
      );
      this.backdropNode.appendChild(this.dialogNode);
    }
    this.backdropNode.classList.add("active");

    // Disable scroll on the body element
    document.body.classList.add(compliantDialog.Utils.dialogOpenClass);

    if (typeof focusAfterClosed === "string") {
      this.focusAfterClosed = document.getElementById(focusAfterClosed);
    } else if (typeof focusAfterClosed === "object") {
      this.focusAfterClosed = focusAfterClosed;
    } else {
      throw new Error(
        "the focusAfterClosed parameter is required for the compliantDialog.Dialog constructor."
      );
    }

    if (typeof focusFirst === "string") {
      this.focusFirst = document.getElementById(focusFirst);
    } else if (typeof focusFirst === "object") {
      this.focusFirst = focusFirst;
    } else {
      this.focusFirst = null;
    }

    // Bracket the dialog node with two invisible, focusable nodes.
    // While this dialog is open, we use these to make sure that focus never
    // leaves the document even if dialogNode is the first or last node.
    const preDiv = document.createElement("div");
    this.preNode = this.dialogNode.parentNode.insertBefore(
      preDiv,
      this.dialogNode
    );
    this.preNode.tabIndex = 0;
    const postDiv = document.createElement("div");
    this.postNode = this.dialogNode.parentNode.insertBefore(
      postDiv,
      this.dialogNode.nextSibling
    );
    this.postNode.tabIndex = 0;

    // If this modal is opening on top of one that is already open,
    // get rid of the document focus listener of the open dialog.
    if (compliantDialog.OpenDialogList.length > 0) {
      compliantDialog.getCurrentDialog().removeListeners();
    }

    this.addListeners();
    compliantDialog.OpenDialogList.push(this);
    this.dialogNode.className = "default_dialog"; // make visible

    if (this.focusFirst) {
      this.focusFirst.focus();
    } else {
      compliantDialog.Utils.focusFirstDescendant(this.dialogNode);
    }

    this.lastFocus = document.activeElement;
  }; // end Dialog constructor

  /**
   * @desc
   *  Hides the current top dialog,
   *  removes listeners of the top dialog,
   *  restore listeners of a parent dialog if one was open under the one that just closed,
   *  and sets focus on the element specified for focusAfterClosed.
   */
  compliantDialog.Dialog.prototype.close = function () {
    compliantDialog.OpenDialogList.pop();
    this.removeListeners();
    // this.preNode = null;
    // this.postNode = null;
    // compliantDialog.Utils.remove(this.preNode);
    // compliantDialog.Utils.remove(this.postNode);
    this.dialogNode.className = "hidden";
    this.backdropNode.classList.remove("active");
    if (this.focusAfterClosed) {
      this.focusAfterClosed.focus();
    }

    // If a dialog was open underneath this one, restore its listeners.
    if (compliantDialog.OpenDialogList.length > 0) {
      compliantDialog.getCurrentDialog().addListeners();
    } else {
      document.body.classList.remove(compliantDialog.Utils.dialogOpenClass);
    }
  }; // end close

  compliantDialog.Dialog.prototype.addListeners = function () {
    document.addEventListener("focus", this.trapFocus, true);
  }; // end addListeners

  compliantDialog.Dialog.prototype.removeListeners = function () {
    document.removeEventListener("focus", this.trapFocus, true);
  }; // end removeListeners

  compliantDialog.Dialog.prototype.trapFocus = function (event) {
    if (compliantDialog.Utils.IgnoreUtilFocusChanges) {
      return;
    }
    const currentDialog = compliantDialog.getCurrentDialog();
    if (currentDialog.dialogNode.contains(event.target)) {
      currentDialog.lastFocus = event.target;
    } else {
      compliantDialog.Utils.focusFirstDescendant(currentDialog.dialogNode);
      if (currentDialog.lastFocus == document.activeElement) {
        compliantDialog.Utils.focusLastDescendant(currentDialog.dialogNode);
      }
      currentDialog.lastFocus = document.activeElement;
    }
  }; // end trapFocus

  compliantDialog.openDialog = function openCompliantDialog(
    dialogId,
    focusAfterClosed,
    focusFirst
  ) {
    new compliantDialog.Dialog(dialogId, focusAfterClosed, focusFirst);
  };

  compliantDialog.closeDialog = function (closeButton) {
    var topDialog = compliantDialog.getCurrentDialog();
    if (topDialog.dialogNode.contains(closeButton)) {
      topDialog.close();
    }
  };
})();