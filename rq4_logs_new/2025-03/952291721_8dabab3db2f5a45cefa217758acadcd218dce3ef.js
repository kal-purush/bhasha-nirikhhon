import React, { useState, useEffect } from 'react';
import NoteList from './pages/NoteList';
import NoteForm from './pages/NoteForm';
import NoteDetail from './pages/NoteDetail';
import './index.css';

function App() {
  const [notes, setNotes] = useState([]);
  const [selectedNote, setSelectedNote] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');

  // 从 localStorage 加载笔记
  useEffect(() => {
    const savedNotes = JSON.parse(localStorage.getItem('notes')) || [];
    setNotes(savedNotes);
  }, []);

  // 保存笔记到 localStorage
  useEffect(() => {
    localStorage.setItem('notes', JSON.stringify(notes));
  }, [notes]);

  // 添加笔记
  const addNote = (title, content) => {
    const newNote = {
      id: Date.now().toString(),
      title,
      content,
    };
    setNotes([...notes, newNote]);
  };

  // 更新笔记
  const updateNote = (id, title, content) => {
    const updatedNotes = notes.map((note) =>
      note.id === id ? { ...note, title, content } : note
    );
    setNotes(updatedNotes);
    setSelectedNote(null); // 清空选中笔记
  };

  // 删除笔记
  const deleteNote = (id) => {
    const filteredNotes = notes.filter((note) => note.id !== id);
    setNotes(filteredNotes);
    setSelectedNote(null); // 清空选中笔记
  };

  // 搜索笔记
  const filteredNotes = notes.filter(
    (note) =>
      note.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      note.content.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="container">
      <div className="search-box">
        <input
          type="text"
          placeholder="搜索笔记..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
        />
      </div>

      <div className="notes-list">
        <h2>笔记列表</h2>
        <NoteList
          notes={filteredNotes}
          onSelectNote={setSelectedNote}
          selectedNoteId={selectedNote?.id}
        />
      </div>

      <div className="add-note">
        <h2>添加新笔记</h2>
        <NoteForm onSubmit={addNote} />
      </div>

      <div className="note-detail">
        <h2>笔记详情</h2>
        {selectedNote ? (
          <NoteDetail
            note={selectedNote}
            onSave={updateNote}
            onDelete={deleteNote}
          />
        ) : (
          <p>请从左侧选择笔记</p>
        )}
      </div>
    </div>
  );
}

export default App;