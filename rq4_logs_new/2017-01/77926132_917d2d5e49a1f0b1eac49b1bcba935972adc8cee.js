(function featureTestPageLoadsWithEmptyNoteList(){
  var noteList = new NoteList();
  var noteController = new NoteController(noteList);
  brand = document.getElementById("brand");
  title = document.getElementById("list-title");
  app = document.getElementById("app");
  noteController.getHtml();

  assert.isTrue(brand.innerHTML === "Notes App", "Page loads with brand name:");
  assert.isTrue(title.innerHTML === "Notes", "Page loads with list header: ");
  assert.isTrue(app.innerHTML === "<ul></ul>", "Page loads with empty notes list: ");
})();