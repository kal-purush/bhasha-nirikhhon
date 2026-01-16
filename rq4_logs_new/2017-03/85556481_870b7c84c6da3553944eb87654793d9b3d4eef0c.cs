using Modulewijzer.Models;
using iTextSharp.text;
using iTextSharp.text.pdf;
using System.IO;
using System;
using System.Collections.Generic;

namespace Modulewijzer.Converters
{
    public sealed class PdfConverter : IDisposable
    {
        private FileStream m_stream;
        private Document m_doc = new Document();
        private PdfWriter m_writer;
        private Font _bigheader = new Font(Font.FontFamily.HELVETICA, 20, Font.BOLD);
        private Font _smallheader = new Font(Font.FontFamily.HELVETICA, 14, Font.BOLD);
        private List<string> _leraren = new List<string>();

        public PdfConverter(string fileName)
        {
            m_stream = new FileStream(fileName, FileMode.Create, FileAccess.Write);
            m_writer = PdfWriter.GetInstance(m_doc, m_stream);
            m_doc.Open();

        }
        public void AddModule(Module module)
        {
            m_doc.Add(new Paragraph("Studiewijzer", _bigheader));
            m_doc.Add(Chunk.NEWLINE);
            m_doc.Add(new Paragraph("Naam", _smallheader));
            m_doc.Add(new Paragraph(module.Naam));
            m_doc.Add(Chunk.NEWLINE);
            m_doc.Add(new Paragraph("Docenten", _smallheader));
            string s = "";
            for (int i = 0; i < _leraren.Count; i++)
            {
                s += _leraren[i];
                if (i < _leraren.Count - 1) s += ", ";
            }
            m_doc.Add(new Paragraph(s));
            m_doc.Add(Chunk.NEWLINE);
            m_doc.Add(new Paragraph("Jaar van opstellen van document", _smallheader));
            m_doc.Add(new Paragraph(DateTime.Now.Year.ToString()));
            m_doc.Add(Chunk.NEWLINE);
            m_doc.Add(new Paragraph("EC's", _smallheader));
            m_doc.Add(new Paragraph(module.AantalEcs.ToString()));
            m_doc.Add(Chunk.NEWLINE);
            m_doc.Add(new Paragraph("Studiejaar en periode", _smallheader));
            m_doc.Add(new Paragraph(String.Format("Jaar {0} periode {1}", module.StudieJaar, module.Periode)));
            m_doc.Add(Chunk.NEWLINE);
            m_doc.Add(new Paragraph("Leeruitkomsten", _smallheader));
            m_doc.Add(new Paragraph(module.Leeruitkomsten));
            m_doc.Add(Chunk.NEWLINE);
            m_doc.Add(new Paragraph("Competenties", _smallheader));
            // Competenties hier
            m_doc.Add(Chunk.NEWLINE);
            m_doc.Add(new Paragraph("Literatuur", _smallheader));
            m_doc.Add(new Paragraph(module.Literatuur));
            m_doc.Add(Chunk.NEWLINE);
            m_doc.Add(new Paragraph("Planning", _smallheader));
            // Planning
        }
        public void AddLeraar(string naam)
        {

            // Leraar naar Pdf
            // Gebruikt nu string ipv Docent object
            _leraren.Add(naam);
        }
        public void AddCompetentie(Competentie competentie)
        {
            // Competentie naar Pdf
        }

       public void Dispose()
        {
            m_doc.Close();
        } 
    }
}