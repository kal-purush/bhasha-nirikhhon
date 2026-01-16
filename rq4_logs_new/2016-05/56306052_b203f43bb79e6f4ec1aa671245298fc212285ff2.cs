using UnityEngine;
using System.Collections.Generic;
using System.Xml.Linq;
using System;

/// <summary>
/// Represents music score
/// </summary>
public class MusicScore
{
    /// <summary>
    /// Music score data
    /// </summary>
    private List<Note> score;

    /// <summary>
    /// Gets the number of notes
    /// </summary>
    public int Count
    {
        get { return score.Count; }
    }

    /// <summary>
    /// Gets notes
    /// </summary>
    public IList<Note> Notes
    {
        get { return score; }
    }

    /// <summary>
    /// Loads music score from file
    /// </summary>
    /// <param name="filename"></param>
    public void Load(string filename)
    {
        score = new List<Note>();

        try
        {
            // parse xml document
            var asset = (TextAsset)Resources.Load("MusicScores/" + filename);
            var document = XDocument.Parse(asset.text);
            var notesElement = document.Root.Descendants("notes");

            foreach (var noteElement in notesElement.Descendants("note"))
            {
                var note = new Note(
                    int.Parse(noteElement.Attribute("time").Value),
                    int.Parse(noteElement.Attribute("x").Value),
                    int.Parse(noteElement.Attribute("y").Value),
                    (Note.NoteType)Enum.Parse(typeof(Note.NoteType), noteElement.Attribute("type").Value),
                    int.Parse(noteElement.Attribute("point").Value));
                score.Add(note);
            }
        }
        catch (Exception e)
        {
            Debug.LogError("Failed to parse score file. (" + e.Message + ")");
        }
    }

    /// <summary>
    /// Returns a note which is nearest to given time
    /// </summary>
    /// <param name="time">Time</param>
    /// <returns>A note</returns>
    public Note GetNearestNote(float time)
    {
        // TODO: take faster algorithm
        Note result = null;
        float min = float.MaxValue;
        foreach (var note in score)
        {
            var distance = Math.Abs(note.Time - time);
            if (distance < min)
            {
                min = distance;
                result = note;
            }
        }
        return result;
    }
}