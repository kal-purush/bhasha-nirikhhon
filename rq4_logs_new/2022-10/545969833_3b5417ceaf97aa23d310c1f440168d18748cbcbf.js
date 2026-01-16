import taskComponent from './taskComponent.js';

const App = {
  data() {
    return {
      columns: [
        {column: 1, notes: []},
        {column: 2, notes: []},
        {column: 3, notes: []}
      ],
      dragNote: {
        index: '',
        column: ''
      },
      dragClass: 'drag-js' 
    }
  },
  components: {
    'task-component': taskComponent,
  },
  methods: {
    addNote(inputValue, column) {
      this.columns[column - 1].notes.push(inputValue)
    },
    removeNote(index, column) {
      this.columns[column - 1].notes.splice(index, 1);
    },
    startDrag(index, column) {
      this.dragNote.column = column;
      this.dragNote.index = index;
    },
    drop(targetColumn, evt, targetIndex) {
      evt.stopPropagation();
      const dragNote = this.columns[this.dragNote.column - 1].notes[this.dragNote.index];
      const dragNotes = this.columns[this.dragNote.column - 1].notes;
      const targetNotes = this.columns[targetColumn - 1].notes;

      if (targetColumn === this.dragNote.column) {
        targetNotes.splice(this.dragNote.index, 1);

        if (evt.target.classList.contains(this.dragClass)) {

          if (this.dragNote.index < targetIndex - 1) {
            targetNotes.splice(targetIndex - 1, 0, dragNote)
          }
          if (this.dragNote.index >= targetIndex  - 1) {
            targetNotes.splice(targetIndex, 0, dragNote)
          }

        } else {
          targetNotes.push(dragNote);
        }
      };

      if (targetColumn !== this.dragNote.column) {
        dragNotes.splice(this.dragNote.index, 1);

        if (evt.target.classList.contains(this.dragClass)) {
          targetNotes.splice(targetIndex, 0, dragNote);
        } else {
          targetNotes.push(dragNote);
        }
      }
    },
  },
  template: `
    <task-component 
      v-for="col in columns"
      :column="col.column"
      :notes="col.notes"
      :dragClass="dragClass"
      @removeNoteEvent="removeNote"
      @addNodeEvent="addNote"
      @startDragEvent="startDrag"
      @dropEvent="drop"
    ></task-component>
  `,
};

const app = Vue.createApp(App);

// app.component('task-component', { 
//   props: ['column', 'notes', 'dragClass'],
//   emits: ['removeNoteEvent','addNodeEvent', 'startDragEvent', 'dropEvent'],
//   data() {
//     return {
//       inputValue: '',
//     }
//   },
//   methods: {
//     inputChange(event) {
//       this.inputValue = event.target.value;
//     },
//     validateInput(event) {
//        if (event.target.value.length >= 15) {
//         event.preventDefault();
//       }
//     },
//     addNote() {
//       if (this.inputValue !== '') {
//         this.$emit('addNodeEvent', this.inputValue, this.column);
//         this.inputValue = '';
//       }
//     },
//     enterDragEvent(evt) {
//       evt.target.classList.add('activeItem');
//     },
//     leaveDragEvent(evt) {
//       evt.target.classList.remove('activeItem');
//     }
//   },
//   template: 
//   `
//     <div class="column"
//       @drop="$emit('dropEvent', column, $event, index)"
//       @dragover.prevent
//       @dragenter.prevent
//     >
//       <div class="column__head">
//         <input
//           type="text" 
//           placeholder="enter note"
//           :value="inputValue"
//           @input="inputChange" 
//           @keypress.enter="addNote"
//           @keypress="validateInput"
//         >
//         <button @click="addNote">add note</button>
//       </div>
//       <ul>
//         <li
//           v-for="(note, index) in notes"
//         >
//           <div 
//             class="content"
//             :class="dragClass"
//             draggable="true"
//             @drop="leaveDragEvent($event), $emit('dropEvent', column, $event, index)"
//             @dragover.prevent
//             @dragenter.prevent
//             @dragstart="$emit('startDragEvent', index, column, $event)"
//             @dragenter="enterDragEvent($event)"
//             @dragleave="leaveDragEvent($event)"
//           >{{ note }}</div>
//           <button @click="$emit('removeNoteEvent', index, column)">remove</button>
//         </li>
//       </ul>
//     </div>
//   `
// });

app.mount('#app');